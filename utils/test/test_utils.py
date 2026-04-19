import os
import sys
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import get_db_connection


class TestDBConnection:

    def test_connection_success(self):
        """Test that connection is established successfully"""
        conn = get_db_connection()
        assert conn is not None
        assert conn.closed == 0  # 0 means open
        conn.close()

    def test_connection_is_valid(self):
        """Test that connection can execute a query"""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1
        cur.close()
        conn.close()

    def test_connection_wrong_section(self):
        """Test that wrong section raises exception"""
        with pytest.raises(Exception) as exc_info:
            get_db_connection(section="wrong_section")
        assert "wrong_section" in str(exc_info.value)

    def test_connection_wrong_config_file(self):
        """Test that wrong config file raises exception"""
        with pytest.raises(Exception):
            get_db_connection(config_file="wrong_path/configs.yaml")


    def test_connection_closes_properly(self):
        """Test that connection closes properly"""
        conn = get_db_connection()
        conn.close()
        assert conn.closed == 1  # 1 means closed

    def test_ops_sys_schema_exists(self):
        """Test that ops_sys schema exists in the database"""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'ops_sys'
        """)
        result = cur.fetchone()
        assert result is not None, "ops_sys schema does not exist"
        cur.close()
        conn.close()
