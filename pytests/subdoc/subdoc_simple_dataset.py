from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
from subdoc_base import SubdocBaseTest
import copy, json
import random

class SubdocSimpleDataset(SubdocBaseTest):
    def setUp(self):
        super(SubdocSimpleDataset, self).setUp()
        self.client = self.direct_client(self.master, self.buckets[0])

    def tearDown(self):
        super(SubdocSimpleDataset, self).tearDown()

    # Test the fix for MB-30278
    def test_verify_backtick(self):
        result = True
        dict = {}
        self.key = "verify_backtick"
        array = {
            "name`": "Douglas Reynholm",
            "place": "India",
        }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)

        # Insert double backtick(``) to refer a literal backtick(`) in key
        for count in range(5):
            key1 = 'name``'
            try:
                opaque, cas, data = self.client.get_sd(self.key, key1)
                data = json.loads(data)
                if data != array["name`"]:
                    self.fail("Data does not match")
            except Exception as e:
                self.log("Unable to get key {} for path {} after {} tries".format(self.key, key1, count))
                result = False

        self.assertTrue(result, dict)

#SD_COUNTER
    def test_counter(self):
        result = True
        dict = {}
        self.key = "test_counter"
        array = {
                    "add_integer":0,
                    "sub_integer":1,
                    "add_double":0.0,
                    "sub_double":0.0,
                    "array_add_integer":[0,1],
                    "array_sub_integer":[0,1],
                }
        expected_array = {
                    "add_integer":1,
                    "sub_integer":0,
                    "add_double":1.0,
                    "sub_double":0.0,
                    "array_add_integer":[1,1],
                    "array_sub_integer":[0,0],
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.counter(self.client, key = "test_counter", path = 'add_integer', value = "1")
        self.counter(self.client, key = "test_counter", path = 'sub_integer', value = "-1")
        #self.counter(self.client, key = "test_counter", path = 'add_double', value = "1.0")
        #self.counter(self.client, key = "test_counter", path = 'sub_double', value = "-1.0")
        self.counter(self.client, key = "test_counter", path = 'array_add_integer[0]', value = "-1.0")
        self.counter(self.client, key = "test_counter", path = 'array_add_integer[1]', value = "-1.0")
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# SD_GET

    def test_get_numbers(self):
    	self.json =  self.generate_simple_data_numbers()
        self.get_verify(self.json, "simple_dataset_numbers")

    def test_get_null(self):
        self.json =  self.generate_simple_data_null()
        self.get_verify(self.json, "simple_dataset_null")

    def test_get_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.get_verify(self.json, "simple_dataset_boolean")

    def test_get_array_numbers(self):
    	self.json =  self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "simple_dataset_array_numbers")

    def test_get_array_strings(self):
    	self.json =  self.generate_simple_data_strings()
        self.get_verify(self.json, "generate_simple_data_array_strings")

    def test_get_mix_arrays(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.get_verify(self.json, "generate_simple_data_mix_arrays")
    def test_get_numbers_boundary(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.get_verify(self.json, "generate_simple_data_numbers_boundary")

    def test_get_element_arrays(self):
        self.key = "element_arrays"
        self.json =  self.generate_simple_arrays()
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        key_single_dimension_path = "single_dimension_array[0]"
        self.get_and_verify_with_value(self.client, self.key, key_single_dimension_path, str(self.json["single_dimension_array"][0]))
        key_two_dimension_path = "two_dimension_array[0][0]"
        self.get_and_verify_with_value(self.client, self.key, key_two_dimension_path, str(self.json["two_dimension_array"][0][0]))
        self.assertTrue(result, dict)

# SD_ARRAY_ADD

    def test_add_last_array(self):
        result = True
        dict = {}
        self.key = "test_add_last_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["0"],
                    "two_dimension_array":[["0"]],
                    "three_dimension_array":[[["0"]]]
                }
        expected_array = {
                    "empty":["1"],
                    "single_dimension_array":["0","1"],
                    "two_dimension_array":[["0", "1"]],
                    "three_dimension_array":[[["0", "1"]]]
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_last(self.client, key = "test_add_last_array", path = 'empty', value = json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'single_dimension_array', value =  json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'two_dimension_array[0]', value =  json.dumps("1"))
        self.array_add_last(self.client, key = "test_add_last_array", path = 'three_dimension_array[0][0]', value =  json.dumps("1"))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_first_array(self):
        result = True
        dict = {}
        self.key = "test_add_first_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["1"],
                    "two_dimension_array":[["1"]],
                    "three_dimension_array":[[["1"]]]
                }
        expected_array = {
                    "empty":["0"],
                    "single_dimension_array":["0","1"],
                    "two_dimension_array":[["0", "1"]],
                    "three_dimension_array":[[["0", "1"]]]
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_first(self.client, key = "test_add_first_array", path = 'empty', value = json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'single_dimension_array', value =  json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'two_dimension_array[0]', value =  json.dumps("0"))
        self.array_add_first(self.client, key = "test_add_first_array", path = 'three_dimension_array[0][0]', value =  json.dumps("0"))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_unique_array(self):
        result = True
        dict = {}
        self.key = "test_add_unique_array"
        array = {
                    "empty":[],
                    "single_dimension_array":["0",2],
                    "two_dimension_array":[["0",2]],
                    "three_dimension_array":[[["0", 2]]]
                }
        expected_array = {
                    "empty":["1"],
                    "single_dimension_array":["0",2, "1"],
                    "two_dimension_array":[["0",2, "1"]],
                    "three_dimension_array":[[["0",2,"1"]]]
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'empty', value = json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'single_dimension_array', value =  json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'two_dimension_array[0]', value =  json.dumps("1"))
        self.array_add_unique(self.client, key = "test_add_unique_array", path = 'three_dimension_array[0][0]', value =  json.dumps("1"))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

    def test_add_insert_array(self):
        result = True
        dict = {}
        self.key = "test_add_insert_array"
        array = {
                    "single_dimension_array_no_element":[],
                    "two_dimension_array_no_element":[[]],
                    "three_dimension_array_no_element":[[[]]]
                }
        expected_array = {
                    "single_dimension_array_no_element":[0, 1 , 2, 3],
                    "two_dimension_array_no_element":[[0, 1, 2, 3],[0, 1, 2, 3]],
                    "three_dimension_array_no_element":[[[0, 1, 2, 3],[0, 1, 2, 3]],[0, 1, 2, 3]],
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "single_dimension_array_no_element[2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[0][2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "two_dimension_array_no_element[1]", value = json.dumps([0, 1, 2, 3]))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][0]", value = json.dumps(1))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][0]", value = json.dumps(0))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][2]", value = json.dumps(3))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][0][2]", value = json.dumps(2))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[0][1]", value = json.dumps([0, 1, 2, 3]))
        self.array_add_insert(self.client, key  = "test_add_insert_array" , path = "three_dimension_array_no_element[1]", value = json.dumps([0, 1, 2, 3]))
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# SD_ADD

    def test_add_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_numbers")

    def test_add_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_add_verify(self.json, "test_add_array_of_numbers")

    def test_add_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_add_verify(self.json, "test_add_numbers_boundary")

    def test_add_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_add_verify(self.json, "test_add_string")

    def test_add_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_add_verify(self.json, "test_add_array_strings")

    def test_add_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_add_verify(self.json, "test_add_null")

    def test_add_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_add_verify(self.json, "test_add_boolean")

    def test_add_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_add_verify(self.json, "test_add_array_mix")

# SD_UPSERT - Add Operations

    def test_upsert_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers")

    def test_upsert_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_array_of_numbers")

    def test_upsert_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_upsert_verify(self.json, "test_upsert_numbers_boundary")

    def test_upsert_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_upsert_verify(self.json, "test_upsert_string")

    def test_upsert_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_upsert_verify(self.json, "test_upsert_array_strings")

    def test_upsert_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_upsert_verify(self.json, "test_upsert_null")

    def test_upsert_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_upsert_verify(self.json, "test_upsert_boolean")

    def test_upsert_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_upsert_verify(self.json, "test_upsert_array_mix")

# SD_UPERT - Replace Operations

    def test_upsert_replace_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers")

    def test_upsert_replace_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_of_numbers")

    def test_upsert_replace_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_numbers_boundary")

    def test_upsert_replace_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_string")

    def test_upsert_replace_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_strings")

    def test_upsert_replace_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_null")

    def test_upsert_replace_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_boolean")

    def test_upsert_replace_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_upsert_replace_verify(self.json, "test_upsert_replace_array_mix")

# SD_REPLACE - Replace Operations

    def test_replace_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers")

    def test_replace_array_numbers(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_replace_verify(self.json, "test_replace_array_of_numbers")

    def test_replace_numbers_boundary(self):
        self.json =  self.generate_simple_data_numbers()
        self.dict_replace_verify(self.json, "test_replace_numbers_boundary")

    def test_replace_strings(self):
        self.json =  self.generate_simple_data_strings()
        self.dict_replace_verify(self.json, "test_replace_string")

    def test_replace_array_strings(self):
        self.json =  self.generate_simple_data_array_strings()
        self.dict_replace_verify(self.json, "test_replace_array_strings")

    def test_replace_null(self):
        self.json =  self.generate_simple_data_null()
        self.dict_replace_verify(self.json, "test_replace_null")

    def test_replace_boolean(self):
        self.json =  self.generate_simple_data_boolean()
        self.dict_replace_verify(self.json, "test_replace_boolean")

    def test_replace_array_mix(self):
        self.json =  self.generate_simple_data_mix_arrays()
        self.dict_replace_verify(self.json, "test_replace_array_mix")

# SD_DELETE

    def test_delete_dict(self):
        self.json =  self.generate_simple_data_array_of_numbers()
        self.dict_delete_verify(self.json, "test_delete_array")

    def test_delete_array(self):
        result = True
        self.key = "test_delete_array"
        array = {
                    "numbers":[1,2,3],
                    "strings":["absde", "dddl", "dkdkd"],
                    "two_dimension_array":[["0","1"]],
                    "three_dimension_array":[[["0","1"]]]
                }
        expected_array = {
                    "numbers":[2,3],
                    "strings":["absde", "dddl"],
                    "two_dimension_array":[["1"]],
                    "three_dimension_array":[[["1"]]]
                }
        jsonDump = json.dumps(array)
        self.client.set(self.key, 0, 0, jsonDump)
        self.delete(self.client, key = "test_delete_array", path = 'numbers[0]')
        self.delete(self.client, key = "test_delete_array", path = 'strings[2]')
        self.delete(self.client, key = "test_delete_array", path = 'two_dimension_array[0][0]')
        self.delete(self.client, key = "test_delete_array", path = 'three_dimension_array[0][0][0]')
        self.json  = expected_array
        for key in expected_array.keys():
            value = expected_array[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": expected_array[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)

# Helper Methods

    def get_verify(self, dataset, data_key = "default"):
        dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = self.json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                dict[key] = {"expected": self.json[key], "actual": data_return}
            result = result and logic
        self.assertTrue(result, dict)


    def dict_add_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(dict)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = self.json[key]
            value = json.dumps(value)
            self.dict_add(self.client, self.key, key, value)
        for key in self.json.keys():
            value = self.json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                result_dict[key] = {"expected":self.json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(dict)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = self.json[key]
            value = json.dumps(value)
            self.dict_upsert(self.client, self.key, key, value)
        for key in self.json.keys():
            value = self.json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                result_dict[key] = {"expected":self.json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_upsert_replace_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        new_json = self.shuffle_json(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = new_json[key]
            value = json.dumps(value)
            self.dict_upsert(self.client, self.key, key, value)
        self.json =  new_json
        for key in new_json.keys():
            value = new_json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                result_dict[key] = {"expected":self.new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_replace_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        new_json = self.shuffle_json(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            value = new_json[key]
            value = json.dumps(value)
            self.dict_replace(self.client, self.key, key, value)
        self.json =  new_json
        for key in new_json.keys():
            value = new_json[key]
            logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
            if not logic:
                result_dict[key] = {"expected":self.new_json[key], "actual":data_return}
            result = result and logic
        self.assertTrue(result, result_dict)

    def dict_delete_verify(self, dataset, data_key = "default"):
        dict = {}
        result_dict = {}
        result = True
        self.key = data_key
        self.json =  dataset
        jsonDump = json.dumps(self.json)
        self.client.set(self.key, 0, 0, jsonDump)
        for key in self.json.keys():
            self.delete(self.client, self.key, key)
            self.json.pop(key)
            for key in self.json.keys():
                value = self.json[key]
                logic, data_return  =  self.get_string_and_verify_return( self.client, key = self.key, path = key)
                if not logic:
                    result_dict[key] = {"expected":self.new_json[key], "actual":data_return}
                    result = result and logic
            self.assertTrue(result, result_dict)

    def delete(self, client, key = '', path = ''):
        try:
            self.client.delete_sd(key, path)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_add(self, client, key = '', path = '', value = None):
        try:
            self.client.dict_add_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_replace(self, client, key = '', path = '', value = None):
        try:
            self.client.replace_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to replace key {0} for path {1} after {2} tries".format(key, path, 1))

    def dict_upsert(self, client, key = '', path = '', value = None):
        try:
            self.client.dict_upsert_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def counter(self, client, key = '', path = '', value = None):
        try:
            self.client.counter_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_last(self, client, key = '', path = '', value = None):
        try:
            self.client.array_push_last_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_first(self, client, key = '', path = '', value = None):
        try:
            self.client.array_push_first_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_unique(self, client, key = '', path = '', value = None):
        try:
            self.client.array_add_unique_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def array_add_insert(self, client, key = '', path = '', value = None):
        try:
            self.client.array_add_insert_sd(key, path, value)
        except Exception as e:
            self.log.info(e)
            self.fail("Unable to add key {0} for path {1} after {2} tries".format(key, path, 1))

    def get_and_verify(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        self.assertTrue(str(data) == str(self.json[path]), msg="data not returned correctly")

    def get_and_verify_with_value(self, client, key = '', path = '', value = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
            data = json.loads(data)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        self.assertTrue(str(data) == value, msg="data not returned correctly")

    def get_string_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
            data = json.loads(data)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return data == self.json[path], data

    def get_and_verify_return(self, client, key = '', path = ''):
        try:
            opaque, cas, data = self.client.get_sd(key, path)
        except Exception as e:
            self.fail("Unable to get key {0} for path {1} after {2} tries".format(key, path, 1))
        return str(data) == str(self.json[path]), data

    def shuffle_json(self, json_value):
        dict = {}
        keys = json_value.keys()
        for key in keys:
            index = random.randint(0,len(keys)-1)
            dict[key] =json_value[keys[index]]
        return dict

