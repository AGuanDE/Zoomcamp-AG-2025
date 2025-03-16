import unittest
from unittest.mock import MagicMock, patch
from pyflink.table import StreamTableEnvironment, Table
from config import JobConfig
from session_job import create_events_source_kafka, create_events_aggregated_sink, log_aggregation

class TestSessionJob(unittest.TestCase):
    def setUp(self):
        self.mock_env = MagicMock()
        self.mock_t_env = MagicMock(spec=StreamTableEnvironment)
        self.config = JobConfig()
        
    def test_create_events_source_kafka(self):
        # Test creating Kafka source table
        table_name = create_events_source_kafka(self.mock_t_env, self.config)
        
        # Verify execute_sql was called
        self.mock_t_env.execute_sql.assert_called_once()
        
        # Verify the table name is returned
        self.assertEqual(table_name, "green_taxi_source")
        
        # Verify the DDL contains required fields
        ddl = self.mock_t_env.execute_sql.call_args[0][0]
        self.assertIn("lpep_pickup_datetime VARCHAR", ddl)
        self.assertIn("lpep_dropoff_datetime VARCHAR", ddl)
        self.assertIn("PULocationID INTEGER", ddl)
        self.assertIn("DOLocationID INTEGER", ddl)
        
    def test_create_events_aggregated_sink(self):
        # Test creating PostgreSQL sink table
        table_name = create_events_aggregated_sink(self.mock_t_env, self.config)
        
        # Verify execute_sql was called
        self.mock_t_env.execute_sql.assert_called_once()
        
        # Verify the table name matches config
        self.assertEqual(table_name, self.config.postgres_config['table'])
        
        # Verify the DDL contains required fields
        ddl = self.mock_t_env.execute_sql.call_args[0][0]
        self.assertIn("PULocationID INTEGER", ddl)
        self.assertIn("DOLocationID INTEGER", ddl)
        self.assertIn("trips BIGINT", ddl)
        
    @patch('session_job.StreamExecutionEnvironment')
    @patch('session_job.StreamTableEnvironment')
    def test_log_aggregation(self, mock_stream_table_env, mock_stream_env):
        # Setup mocks
        mock_env = MagicMock()
        mock_t_env = MagicMock()
        mock_stream_env.get_execution_environment.return_value = mock_env
        mock_stream_table_env.create.return_value = mock_t_env
        
        # Test the main job function
        log_aggregation()
        
        # Verify environment setup
        mock_env.enable_checkpointing.assert_called_once_with(
            self.config.flink_config['checkpoint_interval']
        )
        mock_env.set_parallelism.assert_called_once_with(
            self.config.flink_config['parallelism']
        )
        
        # Verify table creation and query execution
        self.assertTrue(mock_t_env.execute_sql.called)
        
if __name__ == '__main__':
    unittest.main() 