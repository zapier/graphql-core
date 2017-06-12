import pytest

from graphql.error import format_error
from graphql.execution import execute
from graphql.language.parser import parse
from graphql.type import (GraphQLField, GraphQLNonNull, GraphQLObjectType,
                          GraphQLSchema, GraphQLString)


pytestmark = pytest.mark.asyncio

sync_error = Exception('sync')
non_null_sync_error = Exception('nonNullSync')
promise_error = Exception('promise')
non_null_promise_error = Exception('nonNullPromise')


class ThrowingData(object):

    def sync(self):
        raise sync_error

    def nonNullSync(self):
        raise non_null_sync_error

    async def promise(self):
        raise promise_error

    async def nonNullPromise(self):
        raise non_null_promise_error

    def nest(self):
        return ThrowingData()

    def nonNullNest(self):
        return ThrowingData()

    async def promiseNest(self):
        return ThrowingData()

    async def nonNullPromiseNest(self):
        return ThrowingData()


class NullingData(object):

    def sync(self):
        return None

    def nonNullSync(self):
        return None

    async def promise(self):
        return None

    async def nonNullPromise(self):
        return None

    def nest(self):
        return NullingData()

    def nonNullNest(self):
        return NullingData()

    async def promiseNest(self):
        return NullingData()

    async def nonNullPromiseNest(self):
        return NullingData()


DataType = GraphQLObjectType('DataType', lambda: {
    'sync': GraphQLField(GraphQLString),
    'nonNullSync': GraphQLField(GraphQLNonNull(GraphQLString)),
    'promise': GraphQLField(GraphQLString),
    'nonNullPromise': GraphQLField(GraphQLNonNull(GraphQLString)),
    'nest': GraphQLField(DataType),
    'nonNullNest': GraphQLField(GraphQLNonNull(DataType)),
    'promiseNest': GraphQLField(DataType),
    'nonNullPromiseNest': GraphQLField(GraphQLNonNull(DataType))
})

schema = GraphQLSchema(DataType)


def order_errors(error):
    locations = error['locations']
    return (locations[0]['column'], locations[0]['line'])


async def check(doc, data, expected):
    ast = parse(doc)
    response = await execute(schema, ast, data)

    if response.errors:
        result = {
            'data': response.data,
            'errors': [format_error(e) for e in response.errors]
        }
        if result['errors'] != expected['errors']:
            assert result['data'] == expected['data']
            # Sometimes the fields resolves asynchronously, so
            # we need to check that the errors are the same, but might be
            # raised in a different order.
            assert sorted(result['errors'], key=order_errors) == sorted(expected['errors'], key=order_errors)
        else:
            assert result == expected
    else:
        result = {
            'data': response.data
        }

        assert result == expected


async def test_nulls_a_nullable_field_that_throws_sync():
    doc = '''
        query Q {
            sync
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'sync': None},
        'errors': [{'locations': [{'column': 13, 'line': 3}], 'message': str(sync_error)}]
    })


async def test_nulls_a_nullable_field_that_throws_in_a_promise():
    doc = '''
        query Q {
            promise
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'promise': None},
        'errors': [{'locations': [{'column': 13, 'line': 3}], 'message': str(promise_error)}]
    })


async def test_nulls_a_sync_returned_object_that_contains_a_non_nullable_field_that_throws():
    doc = '''
        query Q {
            nest {
                nonNullSync,
            }
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'nest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': str(non_null_sync_error)}]
    })


async def test_nulls_a_synchronously_returned_object_that_contains_a_non_nullable_field_that_throws_in_a_promise():
    doc = '''
        query Q {
            nest {
                nonNullPromise,
            }
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'nest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': str(non_null_promise_error)}]
    })


async def test_nulls_an_object_returned_in_a_promise_that_contains_a_non_nullable_field_that_throws_synchronously():
    doc = '''
        query Q {
            promiseNest {
                nonNullSync,
            }
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'promiseNest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': str(non_null_sync_error)}]
    })


async def test_nulls_an_object_returned_in_a_promise_that_contains_a_non_nullable_field_that_throws_in_a_promise():
    doc = '''
        query Q {
            promiseNest {
                nonNullPromise,
            }
        }
    '''

    await check(doc, ThrowingData(), {
        'data': {'promiseNest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': str(non_null_promise_error)}]
    })


async def test_nulls_a_complex_tree_of_nullable_fields_that_throw():
    doc = '''
      query Q {
        nest {
          sync
          promise
          nest {
            sync
            promise
          }
          promiseNest {
            sync
            promise
          }
        }
        promiseNest {
          sync
          promise
          nest {
            sync
            promise
          }
          promiseNest {
            sync
            promise
          }
        }
      }
    '''
    await check(doc, ThrowingData(), {
        'data': {'nest': {'nest': {'promise': None, 'sync': None},
                          'promise': None,
                          'promiseNest': {'promise': None, 'sync': None},
                          'sync': None},
                 'promiseNest': {'nest': {'promise': None, 'sync': None},
                                 'promise': None,
                                 'promiseNest': {'promise': None, 'sync': None},
                                 'sync': None}},
        'errors': [{'locations': [{'column': 11, 'line': 4}], 'message': str(sync_error)},
                   {'locations': [{'column': 11, 'line': 5}], 'message': str(promise_error)},
                   {'locations': [{'column': 13, 'line': 7}], 'message': str(sync_error)},
                   {'locations': [{'column': 13, 'line': 8}], 'message': str(promise_error)},
                   {'locations': [{'column': 13, 'line': 11}], 'message': str(sync_error)},
                   {'locations': [{'column': 13, 'line': 12}], 'message': str(promise_error)},
                   {'locations': [{'column': 11, 'line': 16}], 'message': str(sync_error)},
                   {'locations': [{'column': 11, 'line': 17}], 'message': str(promise_error)},
                   {'locations': [{'column': 13, 'line': 19}], 'message': str(sync_error)},
                   {'locations': [{'column': 13, 'line': 20}], 'message': str(promise_error)},
                   {'locations': [{'column': 13, 'line': 23}], 'message': str(sync_error)},
                   {'locations': [{'column': 13, 'line': 24}], 'message': str(promise_error)}]
    })


async def test_nulls_the_first_nullable_object_after_a_field_throws_in_a_long_chain_of_fields_that_are_non_null():
    doc = '''
    query Q {
        nest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullSync
                }
              }
            }
          }
        }
        promiseNest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullSync
                }
              }
            }
          }
        }
        anotherNest: nest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullPromise
                }
              }
            }
          }
        }
        anotherPromiseNest: promiseNest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullPromise
                }
              }
            }
          }
        }
      }
    '''
    await check(doc, ThrowingData(), {
        'data': {'nest': None, 'promiseNest': None, 'anotherNest': None, 'anotherPromiseNest': None},
        'errors': [{'locations': [{'column': 19, 'line': 8}],
                    'message': str(non_null_sync_error)},
                   {'locations': [{'column': 19, 'line': 19}],
                    'message': str(non_null_sync_error)},
                   {'locations': [{'column': 19, 'line': 30}],
                    'message': str(non_null_promise_error)},
                   {'locations': [{'column': 19, 'line': 41}],
                    'message': str(non_null_promise_error)}]
    })


async def test_nulls_a_nullable_field_that_returns_null():
    doc = '''
        query Q {
            sync
        }
    '''

    await check(doc, NullingData(), {
        'data': {'sync': None}
    })


async def test_nulls_a_nullable_field_that_returns_null_in_a_promise():
    doc = '''
        query Q {
            promise
        }
    '''

    await check(doc, NullingData(), {
        'data': {'promise': None}
    })


async def test_nulls_a_sync_returned_object_that_contains_a_non_nullable_field_that_returns_null_synchronously():
    doc = '''
        query Q {
            nest {
                nonNullSync,
            }
        }
    '''
    await check(doc, NullingData(), {
        'data': {'nest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': 'Cannot return null for non-nullable field DataType.nonNullSync.'}]
    })


async def test_nulls_a_synchronously_returned_object_that_contains_a_non_nullable_field_that_returns_null_in_a_promise():
    doc = '''
        query Q {
            nest {
                nonNullPromise,
            }
        }
    '''
    await check(doc, NullingData(), {
        'data': {'nest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': 'Cannot return null for non-nullable field DataType.nonNullPromise.'}]
    })


async def test_nulls_an_object_returned_in_a_promise_that_contains_a_non_nullable_field_that_returns_null_synchronously():
    doc = '''
        query Q {
            promiseNest {
                nonNullSync,
            }
        }
    '''
    await check(doc, NullingData(), {
        'data': {'promiseNest': None},
        'errors': [{'locations': [{'column': 17, 'line': 4}],
                    'message': 'Cannot return null for non-nullable field DataType.nonNullSync.'}]
    })


async def test_nulls_an_object_returned_in_a_promise_that_contains_a_non_nullable_field_that_returns_null_ina_a_promise():
    doc = '''
        query Q {
            promiseNest {
                nonNullPromise
            }
        }
    '''

    await check(doc, NullingData(), {
        'data': {'promiseNest': None},
        'errors': [
            {'locations': [{'column': 17, 'line': 4}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullPromise.'}
        ]
    })


async def test_nulls_a_complex_tree_of_nullable_fields_that_returns_null():
    doc = '''
      query Q {
        nest {
          sync
          promise
          nest {
            sync
            promise
          }
          promiseNest {
            sync
            promise
          }
        }
        promiseNest {
          sync
          promise
          nest {
            sync
            promise
          }
          promiseNest {
            sync
            promise
          }
        }
      }
    '''
    await check(doc, NullingData(), {
        'data': {
            'nest': {
                'sync': None,
                'promise': None,
                'nest': {
                    'sync': None,
                    'promise': None,
                },
                'promiseNest': {
                    'sync': None,
                    'promise': None,
                }
            },
            'promiseNest': {
                'sync': None,
                'promise': None,
                'nest': {
                    'sync': None,
                    'promise': None,
                },
                'promiseNest': {
                    'sync': None,
                    'promise': None,
                }
            }
        }
    })


async def test_nulls_the_first_nullable_object_after_a_field_returns_null_in_a_long_chain_of_fields_that_are_non_null():
    doc = '''
      query Q {
        nest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullSync
                }
              }
            }
          }
        }
        promiseNest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullSync
                }
              }
            }
          }
        }
        anotherNest: nest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullPromise
                }
              }
            }
          }
        }
        anotherPromiseNest: promiseNest {
          nonNullNest {
            nonNullPromiseNest {
              nonNullNest {
                nonNullPromiseNest {
                  nonNullPromise
                }
              }
            }
          }
        }
      }
    '''

    await check(doc, NullingData(), {
        'data': {
            'nest': None,
            'promiseNest': None,
            'anotherNest': None,
            'anotherPromiseNest': None
        },
        'errors': [
            {'locations': [{'column': 19, 'line': 8}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullSync.'},
            {'locations': [{'column': 19, 'line': 19}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullSync.'},
            {'locations': [{'column': 19, 'line': 30}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullPromise.'},
            {'locations': [{'column': 19, 'line': 41}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullPromise.'}
        ]
    })


async def test_nulls_the_top_level_if_sync_non_nullable_field_throws():
    doc = '''
        query Q { nonNullSync }
    '''
    await check(doc, ThrowingData(), {
        'data': None,
        'errors': [
            {'locations': [{'column': 19, 'line': 2}],
             'message': str(non_null_sync_error)}
        ]
    })


async def test_nulls_the_top_level_if_async_non_nullable_field_errors():
    doc = '''
        query Q { nonNullPromise }
    '''

    await check(doc, ThrowingData(), {
        'data': None,
        'errors': [
            {'locations': [{'column': 19, 'line': 2}],
             'message': str(non_null_promise_error)}
        ]
    })


async def test_nulls_the_top_level_if_sync_non_nullable_field_returns_null():
    doc = '''
        query Q { nonNullSync }
    '''
    await check(doc, NullingData(), {
        'data': None,
        'errors': [
            {'locations': [{'column': 19, 'line': 2}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullSync.'}
        ]
    })


async def test_nulls_the_top_level_if_async_non_nullable_field_resolves_null():
    doc = '''
        query Q { nonNullPromise }
    '''
    await check(doc, NullingData(), {
        'data': None,
        'errors': [
            {'locations': [{'column': 19, 'line': 2}],
             'message': 'Cannot return null for non-nullable field DataType.nonNullPromise.'}
        ]
    })
