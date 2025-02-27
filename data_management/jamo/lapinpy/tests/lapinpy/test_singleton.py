import unittest
from lapinpy.singleton import Singleton


class TestSingleton(unittest.TestCase):

    def test_Singleton_instance(self):
        @Singleton
        class Foo:
            pass

        self.assertIsNotNone(Foo.Instance())

    def test_Singleton_create_new_instance_fails(self):
        @Singleton
        class Foo:
            pass

        self.assertRaises(TypeError, Foo)


if __name__ == '__main__':
    unittest.main()
