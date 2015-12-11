////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "RLMResults_Private.h"

#import "RLMArray_Private.hpp"
#import "RLMObjectSchema_Private.hpp"
#import "RLMObjectStore.h"
#import "RLMObject_Private.hpp"
#import "RLMObservation.hpp"
#import "RLMProperty_Private.h"
#import "RLMQueryUtil.hpp"
#import "RLMRealm_Private.hpp"
#import "RLMSchema_Private.h"
#import "RLMUtil.hpp"

#import "results.hpp"
#import "external_commit_helper.hpp"

#import <objc/runtime.h>
#import <realm/table_view.hpp>

using namespace realm;

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wincomplete-implementation"
@implementation RLMNotificationToken
@end
#pragma clang diagnostic pop

@interface RLMCancellationToken : RLMNotificationToken
@end

@implementation RLMCancellationToken {
    realm::AsyncQueryCancelationToken _token;
}
- (instancetype)initWithToken:(realm::AsyncQueryCancelationToken)token {
    self = [super init];
    if (self) {
        _token = std::move(token);
    }
    return self;
}

- (void)stop {
    _token = {};
}

@end

static const int RLMEnumerationBufferSize = 16;

@implementation RLMFastEnumerator {
    // The buffer supplied by fast enumeration does not retain the objects given
    // to it, but because we create objects on-demand and don't want them
    // autoreleased (a table can have more rows than the device has memory for
    // accessor objects) we need a thing to retain them.
    id _strongBuffer[RLMEnumerationBufferSize];

    RLMRealm *_realm;
    RLMObjectSchema *_objectSchema;

    // Collection being enumerated. Only one of these two will be valid: when
    // possible we enumerate the collection directly, but when in a write
    // transaction we instead create a frozen TableView and enumerate that
    // instead so that mutating the collection during enumeration works.
    id<RLMFastEnumerable> _collection;
    realm::TableView _tableView;
}

- (instancetype)initWithCollection:(id<RLMFastEnumerable>)collection objectSchema:(RLMObjectSchema *)objectSchema {
    self = [super init];
    if (self) {
        _realm = collection.realm;
        _objectSchema = objectSchema;

        if (_realm.inWriteTransaction) {
            _tableView = [collection tableView];
        }
        else {
            _collection = collection;
            [_realm registerEnumerator:self];
        }
    }
    return self;
}

- (void)dealloc {
    if (_collection) {
        [_realm unregisterEnumerator:self];
    }
}

- (void)detach {
    _tableView = [_collection tableView];
    _collection = nil;
}

- (NSUInteger)countByEnumeratingWithState:(NSFastEnumerationState *)state
                                    count:(NSUInteger)len {
    [_realm verifyThread];
    if (!_tableView.is_attached() && !_collection) {
        @throw RLMException(@"Collection is no longer valid");
    }
    // The fast enumeration buffer size is currently a hardcoded number in the
    // compiler so this can't actually happen, but just in case it changes in
    // the future...
    if (len > RLMEnumerationBufferSize) {
        len = RLMEnumerationBufferSize;
    }

    NSUInteger batchCount = 0, count = state->extra[1];

    Class accessorClass = _objectSchema.accessorClass;
    for (NSUInteger index = state->state; index < count && batchCount < len; ++index) {
        RLMObject *accessor = [[accessorClass alloc] initWithRealm:_realm schema:_objectSchema];
        if (_collection) {
            accessor->_row = (*_objectSchema.table)[[_collection indexInSource:index]];
        }
        else if (_tableView.is_row_attached(index)) {
            accessor->_row = (*_objectSchema.table)[_tableView.get_source_ndx(index)];
        }
        _strongBuffer[batchCount] = accessor;
        batchCount++;
    }

    for (NSUInteger i = batchCount; i < len; ++i) {
        _strongBuffer[i] = nil;
    }

    if (batchCount == 0) {
        // Release our data if we're done, as we're autoreleased and so may
        // stick around for a while
        _collection = nil;
        if (_tableView.is_attached()) {
            _tableView = TableView();
        }
        else {
            [_realm unregisterEnumerator:self];
        }
    }

    state->itemsPtr = (__unsafe_unretained id *)(void *)_strongBuffer;
    state->state += batchCount;
    state->mutationsPtr = state->extra+1;

    return batchCount;
}
@end

//
// RLMResults implementation
//
@implementation RLMResults {
    realm::Results _results;
    RLMRealm *_realm;
}

- (instancetype)initPrivate {
    self = [super init];
    return self;
}

[[gnu::noinline]]
[[noreturn]]
static void throwError(NSString *aggregateMethod) {
    try {
        throw;
    }
    catch (realm::InvalidTransactionException const&) {
        @throw RLMException(@"Cannot modify Results outside of a write transaction");
    }
    catch (realm::IncorrectThreadException const&) {
        @throw RLMException(@"Realm accessed from incorrect thread");
    }
    catch (realm::Results::InvalidatedException const&) {
        @throw RLMException(@"RLMResults has been invalidated");
    }
    catch (realm::Results::DetatchedAccessorException const&) {
        @throw RLMException(@"Object has been invalidated");
    }
    catch (realm::Results::IncorrectTableException const& e) {
        @throw RLMException(@"Object type '%s' does not match RLMResults type '%s'.",
                            e.actual.data(), e.expected.data());
    }
    catch (realm::Results::OutOfBoundsIndexException const& e) {
        @throw RLMException(@"Index %zu is out of bounds (must be less than %zu)",
                            e.requested, e.valid_count);
    }
    catch (realm::Results::UnsupportedColumnTypeException const& e) {
        @throw RLMException(@"%@ is not supported for %@ property '%s'",
                            aggregateMethod,
                            RLMTypeToString((RLMPropertyType)e.column_type),
                            e.column_name.data());
    }
}

template<typename Function>
static auto translateErrors(Function&& f, NSString *aggregateMethod=nil) {
    try {
        return f();
    }
    catch (...) {
        throwError(aggregateMethod);
    }
}

+ (instancetype)resultsWithObjectSchema:(RLMObjectSchema *)objectSchema
                                results:(realm::Results)results {
    RLMResults *ar = [[self alloc] initPrivate];
    ar->_results = std::move(results);
    ar->_realm = objectSchema.realm;
    ar->_objectSchema = objectSchema;
    return ar;
}

static inline void RLMResultsValidateInWriteTransaction(__unsafe_unretained RLMResults *const ar) {
    ar->_realm->_realm->verify_thread();
    ar->_realm->_realm->verify_in_write();
}

- (NSUInteger)count {
    return translateErrors([&] { return _results.size(); });
}

- (NSString *)objectClassName {
    return RLMStringDataToNSString(_results.get_object_type());
}

- (NSUInteger)countByEnumeratingWithState:(NSFastEnumerationState *)state
                                  objects:(__unused __unsafe_unretained id [])buffer
                                    count:(NSUInteger)len {
    __autoreleasing RLMFastEnumerator *enumerator;
    if (state->state == 0) {
        enumerator = [[RLMFastEnumerator alloc] initWithCollection:self objectSchema:_objectSchema];
        state->extra[0] = (long)enumerator;
        state->extra[1] = self.count;
    }
    else {
        enumerator = (__bridge id)(void *)state->extra[0];
    }

    return [enumerator countByEnumeratingWithState:state count:len];
}

- (NSUInteger)indexOfObjectWhere:(NSString *)predicateFormat, ... {
    va_list args;
    RLM_VARARG(predicateFormat, args);
    return [self indexOfObjectWhere:predicateFormat args:args];
}

- (NSUInteger)indexOfObjectWhere:(NSString *)predicateFormat args:(va_list)args {
    return [self indexOfObjectWithPredicate:[NSPredicate predicateWithFormat:predicateFormat
                                                                   arguments:args]];
}

- (NSUInteger)indexOfObjectWithPredicate:(NSPredicate *)predicate {
    if (_results.get_mode() == Results::Mode::Empty) {
        return NSNotFound;
    }

    Query query = translateErrors([&] { return _results.get_query(); });
    RLMUpdateQueryWithPredicate(&query, predicate, _realm.schema, _objectSchema);
    size_t index = query.find();
    if (index == realm::not_found) {
        return NSNotFound;
    }
    return _results.index_of(index);
}

- (id)objectAtIndex:(NSUInteger)index {
    return translateErrors([&] {
        return RLMCreateObjectAccessor(_realm, _objectSchema, _results.get(index));
    });
}

- (id)firstObject {
    auto row = translateErrors([&] { return _results.first(); });
    return row ? RLMCreateObjectAccessor(_realm, _objectSchema, *row) : nil;
}

- (id)lastObject {
    auto row = translateErrors([&] { return _results.last(); });
    return row ? RLMCreateObjectAccessor(_realm, _objectSchema, *row) : nil;
}

- (NSUInteger)indexOfObject:(RLMObject *)object {
    if (!object || (!object->_realm && !object.invalidated)) {
        return NSNotFound;
    }

    return translateErrors([&] {
        return RLMConvertNotFound(_results.index_of(object->_row));
    });
}

- (id)valueForKey:(NSString *)key {
    return translateErrors([&] {
        return RLMCollectionValueForKey(self, key);
    });
}

- (void)setValue:(id)value forKey:(NSString *)key {
    translateErrors([&] { RLMResultsValidateInWriteTransaction(self); });
    RLMCollectionSetValueForKey(self, key, value);
}

- (RLMResults *)objectsWhere:(NSString *)predicateFormat, ... {
    va_list args;
    RLM_VARARG(predicateFormat, args);
    return [self objectsWhere:predicateFormat args:args];
}

- (RLMResults *)objectsWhere:(NSString *)predicateFormat args:(va_list)args {
    return [self objectsWithPredicate:[NSPredicate predicateWithFormat:predicateFormat arguments:args]];
}

- (RLMResults *)objectsWithPredicate:(NSPredicate *)predicate {
    return translateErrors([&] {
        if (_results.get_mode() == Results::Mode::Empty) {
            return self;
        }
        auto query = _results.get_query();
        RLMUpdateQueryWithPredicate(&query, predicate, _realm.schema, _objectSchema);
        return [RLMResults resultsWithObjectSchema:_objectSchema
                                           results:realm::Results(_realm->_realm, std::move(query), _results.get_sort())];
    });
}

- (RLMResults *)sortedResultsUsingProperty:(NSString *)property ascending:(BOOL)ascending {
    return [self sortedResultsUsingDescriptors:@[[RLMSortDescriptor sortDescriptorWithProperty:property ascending:ascending]]];
}

- (RLMResults *)sortedResultsUsingDescriptors:(NSArray *)properties {
    return translateErrors([&] {
        if (_results.get_mode() == Results::Mode::Empty) {
            return self;
        }

        return [RLMResults resultsWithObjectSchema:_objectSchema
                                           results:_results.sort(RLMSortOrderFromDescriptors(_objectSchema, properties))];
    });
}

- (id)objectAtIndexedSubscript:(NSUInteger)index {
    return [self objectAtIndex:index];
}

- (id)aggregate:(NSString *)property method:(util::Optional<Mixed> (Results::*)(size_t))method methodName:(NSString *)methodName {
    size_t column = RLMValidatedProperty(_objectSchema, property).column;
    auto value = translateErrors([&] { return (_results.*method)(column); }, methodName);
    if (!value) {
        return nil;
    }
    return RLMMixedToObjc(*value);
}

- (id)minOfProperty:(NSString *)property {
    return [self aggregate:property method:&Results::min methodName:@"minOfProperty"];
}

- (id)maxOfProperty:(NSString *)property {
    return [self aggregate:property method:&Results::max methodName:@"maxOfProperty"];
}

- (id)sumOfProperty:(NSString *)property {
    return [self aggregate:property method:&Results::sum methodName:@"sumOfProperty"];
}

- (id)averageOfProperty:(NSString *)property {
    return [self aggregate:property method:&Results::average methodName:@"averageOfProperty"];
}

- (void)deleteObjectsFromRealm {
    return translateErrors([&] {
        if (_results.get_mode() == Results::Mode::Table) {
            RLMResultsValidateInWriteTransaction(self);
            RLMClearTable(self.objectSchema);
        }
        else {
            RLMTrackDeletions(_realm, ^{ _results.clear(); });
        }
    });
}

- (NSString *)description {
    const NSUInteger maxObjects = 100;
    NSMutableString *mString = [NSMutableString stringWithFormat:@"RLMResults <0x%lx> (\n", (long)self];
    unsigned long index = 0, skipped = 0;
    for (id obj in self) {
        NSString *sub;
        if ([obj respondsToSelector:@selector(descriptionWithMaxDepth:)]) {
            sub = [obj descriptionWithMaxDepth:RLMDescriptionMaxDepth - 1];
        }
        else {
            sub = [obj description];
        }

        // Indent child objects
        NSString *objDescription = [sub stringByReplacingOccurrencesOfString:@"\n" withString:@"\n\t"];
        [mString appendFormat:@"\t[%lu] %@,\n", index++, objDescription];
        if (index >= maxObjects) {
            skipped = self.count - maxObjects;
            break;
        }
    }

    // Remove last comma and newline characters
    if(self.count > 0)
        [mString deleteCharactersInRange:NSMakeRange(mString.length-2, 2)];
    if (skipped) {
        [mString appendFormat:@"\n\t... %lu objects skipped.", skipped];
    }
    [mString appendFormat:@"\n)"];
    return [NSString stringWithString:mString];
}

- (NSUInteger)indexInSource:(NSUInteger)index {
    return translateErrors([&] { return _results.get(index).get_index(); });
}

- (realm::TableView)tableView {
    return translateErrors([&] { return _results.get_tableview(); });
}

namespace {
class CallbackImpl : public realm::AsyncQueryCallback {
public:
    CallbackImpl(void (^block)(RLMResults *, NSError *), dispatch_queue_t queue, NSString *objectClassName, RLMRealmConfiguration *config)
    : _block(block), _queue(queue), _objectClassName(objectClassName), _config(config) {
        dispatch_queue_set_specific(queue, this, this, nullptr);
    }

    ~CallbackImpl() {
        dispatch_queue_set_specific(_queue, this, nullptr, nullptr);
    }

    void deliver(Results r) override {
        @autoreleasepool {
            // This call can't fail because the SharedRealm is already open
            RLMRealm *realm = [RLMRealm realmWithConfiguration:_config error:nil];
            RLMResults *results = _previousResults;
            if (results) {
                if (results->_realm != realm) {
                    results->_realm = realm;
                    results.objectSchema = realm.schema[_objectClassName];
                }
                results->_results = std::move(r);
            }
            else {
                results = [RLMResults resultsWithObjectSchema:realm.schema[_objectClassName]
                                                      results:std::move(r)];
                _previousResults = results;
            }

            _block(results, nil);
        }
    }

    void error(std::exception_ptr err) override {
        try {
            rethrow_exception(err);
        }
        catch (...) {
            NSError *error;
            RLMRealmTranslateException(&error);
            _block(nil, error);
        }
    }

    void update_ready() override {
        __weak auto block = _block;
        auto config = _config;
        void *key = this;
        dispatch_async(_queue, ^{
            if (dispatch_get_specific(key) != key) {
                // Query was stopped after this was dispatched, so skip opening
                // the Realm pointlessly
                return;
            }

            @autoreleasepool {
                NSError *error;
                RLMRealm *realm = [RLMRealm realmWithConfiguration:config error:&error];
                if (realm) {
                    realm->_realm->read_group();
                    realm->_realm->notify();
                }
                else {
                    block(nil, error);
                }
            }
        });
    }

    bool is_for_current_thread() override {
        return dispatch_get_specific(this) == this;
    }

private:
    void (^_block)(RLMResults *, NSError *);
    const dispatch_queue_t _queue;
    NSString *const _objectClassName;
    RLMRealmConfiguration *const _config;

    __weak RLMResults *_previousResults = nil;
};
}

// The compiler complains about the method's argument type not matching due to
// it not having the generic type attached, but it doesn't seem to be possible
// to actually include the generic type
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmismatched-parameter-types"
- (RLMCancellationToken *)deliverOn:(dispatch_queue_t)queue
                              block:(void (^)(RLMResults *, NSError *))block {
    auto token = _results.async(std::make_unique<CallbackImpl>(block, queue,
                                                               self.objectClassName,
                                                               _realm.configuration));
    return [[RLMCancellationToken alloc] initWithToken:std::move(token)];
}

- (RLMCancellationToken *)deliverOnMainThread:(void (^)(RLMResults *, NSError *))block {
    auto token = _results.async(std::make_unique<CallbackImpl>(block, dispatch_get_main_queue(),
                                                               self.objectClassName,
                                                               _realm.configuration));
    return [[RLMCancellationToken alloc] initWithToken:std::move(token)];
}
#pragma clang diagnostic pop
@end
