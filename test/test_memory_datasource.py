
from pyorm.memory_datasource import DataSet
import test_base

class TestDataSet(DataSet):
    def __init__(self):
        DataSet.__init__(self)
        self.id = 1
    def gen_id(self, gen_name):
        self.id += 1
        return self.id

class AllTests(test_base.BaseTests):
    def setUp(self):
        self.dsa = TestDataSet()
        self.populate_dataset(self.dsa)
    def tearDown(self):
        del self.dsa

