import copy

from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.query_definitions import SQLDefinitionGenerator

QUERY_TEMPLATE = "SELECT {0} FROM %s "

class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def test_create_query_explain_drop_index(self):
        self.use_primary_index= self.input.param("use_primary_index",False)
        self.indexes= self.input.param("indexes","").split(":")
        self.emitFields= self.input.param("emitFields","*").split(":")
        self.whereCondition= self.input.param("whereCondition",None)
        self.emitFields = ",".join(self.emitFields)
        query_template = QUERY_TEMPLATE
        query_template = query_template.format(self.emitFields)
        self.index_name = "test_create_query_explain_drop_index"
        if self.use_primary_index:
            self.run_create_index = False
            self.run_drop_index = False
            self.index_name = "primary"
        if self.whereCondition:
            query_template += " WHERE {0}".format(self.whereCondition)
        query_template = self._translate_where_clause(query_template)
        query_definition = QueryDefinition(
            index_name=self.index_name,
            index_fields = self.indexes,
            query_template = query_template,
            groups = [])
        self.run_multi_operations(
            buckets = self.buckets,
            query_definitions = [query_definition],
            create_index = self.run_create_index, drop_index = self.run_drop_index,
            query_with_explain = self.run_query_with_explain, query = self.run_query)

    def test_multi_create_query_explain_drop_index(self):
        try:
            self._create_index_in_async()
            self.run_doc_ops()
            self._query_explain_in_async()
            self._verify_index_map()
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            tasks = self.async_run_multi_operations(
                buckets = self.buckets,
                query_definitions = self.query_definitions,
                drop_index = self.run_drop_index)
            self._run_tasks(tasks)

    def test_multi_create_query_explain_drop_index_with_mutations(self):
        self.doc_ops=True
        self.test_multi_create_query_explain_drop_index()

    def test_multi_create_query_explain_drop_index_with_concurrent_mutations(self):
        try:
            kvops_tasks = self.async_run_doc_ops()
            self._create_index_in_async()
            # runs operations
            self._run_tasks(kvops_tasks)
            self._query_explain_in_async()
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = self.run_drop_index,
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            self._run_tasks(tasks)

    def test_concurrent_mutations_index_create_query_drop(self):
        self.query_definitions_create_candidates =[]
        self.query_definitions_query_candidates =[]
        scan_vector_ranges = []
        scan_vectors = None
        if self.scan_vector_per_values:
            scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
        if len(scan_vector_ranges) > 0:
            for use_percentage in scan_vector_ranges:
                scan_vectors = self.gen_scan_vector(use_percentage=use_percentage,
                                                    use_random=self.random_scan_vector)
        try:
            self.query_definitions_drop_candidates = copy.deepcopy(self.query_definitions)
            self.query_definitions_create_candidates = copy.deepcopy(self.query_definitions)
            self.query_definitions_query_candidates = copy.deepcopy(self.query_definitions)
            i =0
            for query_definition in self.query_definitions_drop_candidates:
                query_definition.index_name += str(i)+"_drop_candidates"
                i+=1
            for query_definition in self.query_definitions_create_candidates:
                query_definition.index_name += str(i)+"_create_candidates"
                i+=1
            for query_definition in self.query_definitions_query_candidates:
                query_definition.index_name += str(i)+"_query_candidates"
                i+=1
            # Start Mutations
            kvops_tasks = self.async_run_doc_ops()
            # Initialize indexes
            self._create_index_in_async(query_definitions = self.query_definitions_drop_candidates)
            self._create_index_in_async(query_definitions = self.query_definitions_query_candidates)
            self.log.info("<<<<< Run Query Tasks >>>>>>")
            query_tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions=self.query_definitions_query_candidates,
                    create_index=False, drop_index=False,
                    query_with_explain=False, query=True, scan_consistency=self.scan_consistency,
                    scan_vectors=scan_vectors)
            self.log.info("<<<<< Run Drop Tasks >>>>>>")
            drop_tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions_drop_candidates,
                    create_index = False, drop_index = True,
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            self._create_index_in_async(query_definitions = self.query_definitions_create_candidates)
            # runs operations
            self._run_tasks(kvops_tasks)
            self._run_tasks(query_tasks)
            self._run_tasks(drop_tasks)
        except Exception, ex:
            self.log.info(ex)
            if not scan_vectors:
                msg = "No scan_vector value"
                if msg not in str(ex):
                    raise
            else:
                raise
        finally:
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions_create_candidates,
                    create_index = False, drop_index = self.run_drop_index,
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            self._run_tasks(tasks)
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions_query_candidates,
                    create_index = False, drop_index = self.run_drop_index,
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            self._run_tasks(tasks)

    def test_multi_create_query_explain_drop_index_primary(self):
        qdfs = []
        for query_definition in self.query_definitions:
            query_definition.index_name = "#primary"
            qdfs.append(query_definition)
        self.query_definitions = qdfs
        try:
            self._verify_primary_index_count()
            self.run_doc_ops()
            self._verify_primary_index_count()
            self._query_explain_in_async()
        except Exception, ex:
            self.log.info(ex)
            raise

    def test_multi_create_query_explain_drop_index_with_index_where_clause(self):
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_employee_data_query_definitions_for_index_where_clause()
        self.use_where_clause_in_index = True
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.test_multi_create_query_explain_drop_index()

    def test_multi_create_query_explain_drop_index_with_index_expressions(self):
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_employee_data_query_definitions_for_index_expressions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.test_multi_create_query_explain_drop_index()

    def test_multi_create_query_explain_drop_index_with_index_expressions_and_where_clause(self):
        self.use_where_clause_in_index = True
        self.test_multi_create_query_explain_drop_index_with_index_expressions()

    def test_multi_create_query_explain_drop_index_scan_consistency_with_index_expressions(self):
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_employee_data_query_definitions_for_index_expressions()
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.test_multi_create_query_explain_drop_index_scan_consistency()

    def test_multi_create_query_explain_drop_index_scan_consistency_with_where_clause(self):
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_employee_data_query_definitions_for_index_where_clause()
        self.use_where_clause_in_index = True
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.test_multi_create_query_explain_drop_index_scan_consistency()

    def test_multi_create_query_explain_drop_index_scan_consistency(self):
        self.random_scan_vector= self.input.param("random_scan_vector",False)
        scan_vector_ranges = []
        scan_vectors = None
        if self.scan_vector_per_values:
            scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
        try:
            self._create_index_in_async()
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                        use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = False)
            self._run_tasks(tasks)
            # runs operations
            self.run_doc_ops()
            if self.scan_vector_per_values:
                scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
            # verify results
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = False)
            self._run_tasks(tasks)
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
        except Exception, ex:
            self.log.info(ex)
            if self.scan_consistency == "at_plus" and not scan_vectors:
                msg = "No scan_vector value"
                if msg not in str(ex):
                    raise
            else:
                raise
        finally:
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = self.run_drop_index,
                query_with_explain = False, query = False)
            self._run_tasks(tasks)

    def test_primary_query_scan_consistency(self):
        self.random_scan_vector= self.input.param("random_scan_vector",False)
        scan_vector_ranges = []
        scan_vectors = None
        if self.scan_vector_per_values:
            scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
        try:
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
            # runs operations
            self.run_doc_ops()
            if self.scan_vector_per_values:
                scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
            self._verify_primary_index_count()
            # verify results
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
        except Exception, ex:
            self.log.info(ex)
            if self.scan_consistency == "at_plus" and not scan_vectors:
                msg = "No scan_vector value"
                if msg not in str(ex):
                    raise
            else:
                raise

    def test_failure_query_with_non_existing_primary_index(self):
        self.indexes= self.input.param("indexes","").split(":")
        self.emitFields= self.input.param("emitFields","*").split(":")
        self.whereCondition= self.input.param("whereCondition",None)
        self.emitFields = ",".join(self.emitFields)
        query_template = QUERY_TEMPLATE
        query_template = query_template.format(self.emitFields)
        self.index_name = "test_failure_query_with_non_existing_primary_index"
        if self.whereCondition:
            query_template += " WHERE {0}".format(self.whereCondition)
        query_template = self._translate_where_clause(query_template)
        query_definition = QueryDefinition(
            index_name=self.index_name,
            index_fields = self.indexes,
            query_template = query_template,
            groups = [])
        try:
            self.run_multi_operations(
            buckets = self.buckets,
            query_definitions = [query_definition],
                create_index = False, drop_index = False,
                query_with_explain = False, query = self.run_query)
            self.fail(" querying without indexes and primary indexes is not allowed")
        except Exception, ex:
            msg = "No primary index on keyspace default. Use CREATE PRIMARY INDEX to create one."
            self.assertTrue(msg in str(ex),"did not recieve message as expected : {0}".format(ex))

    def _generate_scan_vector_ranges(self, scan_vector_per_values = None):
        scan_vector_per_values = str(scan_vector_per_values)
        values = scan_vector_per_values.split(":")
        new_values = []
        for val in values:
            new_values.append(float(val))
        return new_values

    def _run_tasks(self, tasks):
        for task in tasks:
            task.result()

    def _translate_where_clause(self, query):
        query = query.replace("EQUALS","==")
        query = query.replace("NOT_EQUALS","!=")
        query = query.replace("LESS_THAN","<")
        query = query.replace("LESS_THAN_EQUALS","<=")
        query = query.replace("GREATER_THAN",">")
        query = query.replace("GREATER_THAN_EQUALS",">=")
        return query

