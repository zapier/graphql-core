# coding: utf-8
import pytest

from graphql import GraphQLField
from graphql import GraphQLObjectType
from graphql import GraphQLSchema
from graphql import GraphQLString
from graphql import execute
from graphql import parse
from graphql.error import GraphQLLocatedError


pytestmark = pytest.mark.asyncio


async def test_unicode_error_message():
    ast = parse('query Example { unicode }')

    def resolver(context, *_):
        raise Exception(u'UNIÇODÉ!')

    Type = GraphQLObjectType('Type', {
        'unicode': GraphQLField(GraphQLString, resolver=resolver),
    })

    result = await execute(GraphQLSchema(Type), ast)
    assert isinstance(result.errors[0], GraphQLLocatedError)