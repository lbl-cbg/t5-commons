import unittest
from decision import SingleNode, MultipleNode, EndNode
import decision
from parameterized import parameterized


class TestDecision(unittest.TestCase):

    def setUp(self):
        self.single_node = SingleNode(field='foo.bar', values={'a': EndNode(ends=['a', 'b', 'c'])}, complexOnes={
            'foo': [{
                'name': 'foo', 'filter': {}, '__val': 'foo',
                'bar': [{
                    'name': 'bar', 'filter': {}, '__val': 'bar'
                }],
            }],
            'baz': [{
                'name': 'baz', 'filter': {'baz': 1}, '__val': 'baz'}
            ],
        })

    @parameterized.expand([
        ('match', {'foo': {'bar': 'a'}}, 'a'),
        ('mismatch', {'baz': {}}, decision.NULL)
    ])
    def test_SingleNode_getValue(self, _description, value, expected):
        actual = self.single_node.getValue(value)

        self.assertEqual(actual, expected)

    def test_SingleNode_test(self):
        self.assertEqual(self.single_node.test({'foo': {'bar': 'a'}}), ['a', 'b', 'c'])

    def test_MultipleNode_add(self):
        node = MultipleNode()
        node.add(self.single_node)

        self.assertIn(self.single_node, node.nodes)

    def test_MultipleNode_test(self):
        node = MultipleNode()
        node.add(self.single_node)

        self.assertEqual(node.test({'foo': {'bar': 'a'}}), ['a', 'b', 'c'])

    def test_EndNode_test(self):
        node = EndNode(ends=['a', 'b'])

        self.assertEqual(node.test(None), ['a', 'b'])

    @parameterized.expand([
        ('dict', {'foo': 'bar'}, False),
        ('dict_with_$in', {'$in': 'foo'}, True),
        ('str', 'foo', True),
        ('int', 1, True),
        ('float', 1.0, True),
        ('bool', True, True),
    ])
    def test_isSimple(self, _description, value, expected):
        self.assertEqual(decision.isSimple(value), expected)

    @parameterized.expand([
        ('no_existing_subscription', {}, 'foo', {'name': 'foo', 'filter': {}, '__val': 'foo'},
         [{'name': 'foo', 'filter': {}, '__val': 'foo'}]),
        ('not_string_or_int', {}, False, {'name': 'foo', 'filter': {}, '__val': False},
         [{'name': 'foo', 'filter': {}, '__val': False}]),
        ('int', {}, 1, {'name': 'foo', 'filter': {}, '__val': 1}, [{'name': 'foo', 'filter': {}, '__val': 1}]),
        ('existing_subscriptions', {'foo': [{'name': 'foo', 'filter': {}, '__val': 'foo'}]}, 'foo',
         {'name': 'bar', 'filter': {}, '__val': 'bar'}, [{'__val': 'foo', 'filter': {}, 'name': 'foo'},
                                                         {'__val': 'bar', 'filter': {}, 'name': 'bar'}])
    ])
    def test_add(self, _description, data, key, value, expected):
        decision.add(data, key, value)

        self.assertEqual(data.get(key), expected)

    def test_getMostInCommon(self):
        subscriptions = [
            {'name': 'foo', 'filter': {'baa': 1}, '__val': 'foo'},
            {'name': 'bar', 'filter': {'boo': 1}, '__val': 'bar'},
            {'name': 'baz', 'filter': {'baa': 1}, '__val': 'baz'},
        ]

        self.assertEqual(decision.getMostInCommon(subscriptions), 'baa')

    def test_createTree(self):
        subscriptions = [
            {'name': 'foo', 'filter': {'baa': 1}, '__val': 'foo'},
            {'name': 'bar', 'filter': {'boo': 1}, '__val': 'bar'},
            {'name': 'baz', 'filter': {'baa': 1}, '__val': 'baz'},
        ]
        expected = MultipleNode()
        expected.add(SingleNode('baa', {1: ['foo', 'baz']}, {}))
        expected.add(SingleNode('boo', {1: ['bar']}, {}))

        self.assertEqual(str(decision.createTree(subscriptions)), str(expected))

    def test_unifyKeys(self):
        subscriptions = [
            {'name': 'foo', 'filter': {}, '__val': 'foo'},
            {'name': 'bar', 'filter': {'boo': 1}, '__val': 'bar'},
            {'name': 'baz', 'filter': {'baa': 1}, '__val': 'baz'},
        ]
        values = {'foobar': subscriptions}
        node = MultipleNode()
        node.add(SingleNode('boo', {1: ['bar']}, {}))
        node.add(SingleNode('baa', {1: ['baz']}, {}))

        self.assertEqual(str(decision.unifyKeys(values)), str({'foobar': node}))

    def test_createNode(self):
        subscription_1 = {'name': 'foo', 'filter': {}, '__val': 'foo'}
        subscription_2 = {'name': 'bar', 'filter': {'boo': 1}, '__val': 'bar'}
        subscription_3 = {'name': 'baz', 'filter': {'baa': 1}, '__val': 'baz'}
        subscriptions = [subscription_1, subscription_2, subscription_3]

        node, leftovers = decision.createNode(subscriptions)

        self.assertEqual(str(node), str(SingleNode('boo', {1: ['bar']}, {})))
        self.assertEqual(leftovers, [subscription_1, subscription_3])


if __name__ == '__main__':
    unittest.main()
