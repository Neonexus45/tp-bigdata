import streamlit as st
import pandas as pd
import mysql.connector
import sys
import os
from datetime import datetime
import traceback

# Add the project root to Python path
current_file = os.path.abspath(__file__)
dashboard_dir = os.path.dirname(current_file)
src_dir = os.path.dirname(dashboard_dir)
project_root = os.path.dirname(src_dir)
sys.path.insert(0, project_root)

st.set_page_config(
    page_title="Database Connection Test",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

def test_environment_variables():
    """Test if all required environment variables are set"""
    st.subheader("🔧 Environment Variables Test")
    
    required_vars = {
        "Analytics Database": [
            "MYSQL_ANALYTICS_HOST",
            "MYSQL_ANALYTICS_PORT", 
            "MYSQL_ANALYTICS_DATABASE",
            "MYSQL_ANALYTICS_USER",
            "MYSQL_ANALYTICS_PASSWORD"
        ],
        "ML Database": [
            "MYSQL_ML_HOST",
            "MYSQL_ML_PORT",
            "MYSQL_ML_DATABASE", 
            "MYSQL_ML_USER",
            "MYSQL_ML_PASSWORD"
        ]
    }
    
    all_vars_present = True
    
    for db_type, vars_list in required_vars.items():
        st.write(f"**{db_type}:**")
        
        for var in vars_list:
            value = os.getenv(var)
            if value:
                if "PASSWORD" in var:
                    st.success(f"✅ {var}: {'*' * len(value)}")
                else:
                    st.success(f"✅ {var}: {value}")
            else:
                st.error(f"❌ {var}: Not set")
                all_vars_present = False
        
        st.write("")
    
    return all_vars_present

def test_config_import():
    """Test Config class import and initialization"""
    st.subheader("📦 Config Import Test")
    
    try:
        # Direct file import approach
        config_path = os.path.join(project_root, 'src', 'core', 'config.py')
        st.info(f"Project root: {project_root}")
        st.info(f"Looking for config at: {config_path}")
        st.info(f"Config file exists: {os.path.exists(config_path)}")
        
        if os.path.exists(config_path):
            st.success("✅ Config file found")
            
            # Add all necessary paths
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            
            src_path = os.path.join(project_root, 'src')
            if src_path not in sys.path:
                sys.path.insert(0, src_path)
            
            # Import using importlib for better control
            import importlib.util
            spec = importlib.util.spec_from_file_location("config", config_path)
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            
            Config = config_module.Config
            st.success("✅ Config class imported successfully")
            
            # Test Config initialization
            config = Config()
            st.success("✅ Config class initialized successfully")
            
            # Test connection strings
            analytics_conn = config.mysql_analytics_connection_string
            ml_conn = config.mysql_ml_connection_string
            
            st.info(f"Analytics Connection String: {analytics_conn}")
            st.info(f"ML Connection String: {ml_conn}")
            
            return True, config
        else:
            st.error(f"❌ Config file not found at: {config_path}")
            return False, None
        
    except Exception as e:
        st.error(f"❌ Config import/initialization failed: {e}")
        st.code(traceback.format_exc())
        return False, None

def test_mysql_connector():
    """Test MySQL connector import"""
    st.subheader("🔌 MySQL Connector Test")
    
    try:
        import mysql.connector
        st.success(f"✅ MySQL connector imported successfully")
        st.info(f"MySQL Connector version: {mysql.connector.__version__}")
        return True
    except Exception as e:
        st.error(f"❌ MySQL connector import failed: {e}")
        return False

def test_database_connection(db_type, connection_params):
    """Test actual database connection"""
    st.subheader(f"🗄️ {db_type} Database Connection Test")
    
    try:
        # Display connection parameters (hide password)
        display_params = connection_params.copy()
        display_params['password'] = '*' * len(display_params['password'])
        st.json(display_params)
        
        # Test connection
        with st.spinner(f"Connecting to {db_type} database..."):
            connection = mysql.connector.connect(**connection_params)
            st.success(f"✅ {db_type} database connection successful!")
            
            # Test basic operations
            cursor = connection.cursor()
            
            # Get server info
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            st.info(f"MySQL Server Version: {version}")
            
            # List databases
            cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in cursor.fetchall()]
            st.info(f"Available databases: {', '.join(databases)}")
            
            # Check if target database exists
            if connection_params['database'] in databases:
                st.success(f"✅ Target database '{connection_params['database']}' exists")
                
                # List tables in target database
                cursor.execute(f"USE {connection_params['database']}")
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
                
                if tables:
                    st.success(f"✅ Found {len(tables)} tables in database")
                    
                    # Display tables
                    st.write("**Tables:**")
                    for table in tables:
                        st.write(f"- {table}")
                    
                    # Test sample queries on each table
                    st.write("**Table Sample Data:**")
                    for table in tables[:3]:  # Limit to first 3 tables
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cursor.fetchone()[0]
                            st.write(f"- {table}: {count} rows")
                            
                            if count > 0:
                                cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                                sample_data = cursor.fetchall()
                                
                                # Get column names
                                cursor.execute(f"DESCRIBE {table}")
                                columns = [col[0] for col in cursor.fetchall()]
                                
                                # Create DataFrame
                                df = pd.DataFrame(sample_data, columns=columns)
                                st.dataframe(df, use_container_width=True)
                                
                        except Exception as e:
                            st.warning(f"Could not query table {table}: {e}")
                else:
                    st.warning(f"⚠️ No tables found in database '{connection_params['database']}'")
            else:
                st.error(f"❌ Target database '{connection_params['database']}' does not exist")
            
            connection.close()
            return True
            
    except mysql.connector.Error as e:
        st.error(f"❌ {db_type} database connection failed!")
        st.error(f"MySQL Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ {db_type} database connection failed!")
        st.error(f"General Error: {e}")
        st.code(traceback.format_exc())
        return False

def test_data_loaders():
    """Test the actual data loader classes"""
    st.subheader("📊 Data Loaders Test")
    
    try:
        from data_loader import AnalyticsDataLoader, MLDataLoader
        
        # Test Analytics Data Loader
        st.write("**Analytics Data Loader:**")
        try:
            analytics_loader = AnalyticsDataLoader()
            st.success("✅ AnalyticsDataLoader initialized")
            
            # Test a simple query
            with st.spinner("Testing analytics data query..."):
                try:
                    # Try to get weather impact summary
                    data = analytics_loader.get_weather_impact_summary()
                    if not data.empty:
                        st.success(f"✅ Retrieved {len(data)} rows from weather impact analysis")
                        st.dataframe(data.head(), use_container_width=True)
                    else:
                        st.warning("⚠️ No data returned from weather impact analysis")
                except Exception as e:
                    st.error(f"❌ Analytics query failed: {e}")
                    
        except Exception as e:
            st.error(f"❌ AnalyticsDataLoader initialization failed: {e}")
        
        # Test ML Data Loader
        st.write("**ML Data Loader:**")
        try:
            ml_loader = MLDataLoader()
            st.success("✅ MLDataLoader initialized")
            
            # Test a simple query
            with st.spinner("Testing ML data query..."):
                try:
                    # Try to get model performance
                    data = ml_loader.get_model_performance()
                    if not data.empty:
                        st.success(f"✅ Retrieved {len(data)} rows from model performance")
                        st.dataframe(data.head(), use_container_width=True)
                    else:
                        st.warning("⚠️ No data returned from model performance")
                except Exception as e:
                    st.error(f"❌ ML query failed: {e}")
                    
        except Exception as e:
            st.error(f"❌ MLDataLoader initialization failed: {e}")
            
    except ImportError as e:
        st.error(f"❌ Data loader import failed: {e}")

def main():
    st.title("🔍 Database Connection Test")
    st.markdown("This tool tests all database connections and configurations for the Weather Analytics Dashboard.")
    
    # Sidebar controls
    st.sidebar.title("Test Controls")
    
    if st.sidebar.button("🔄 Run All Tests"):
        st.session_state.run_tests = True
    
    test_sections = st.sidebar.multiselect(
        "Select tests to run:",
        [
            "Environment Variables",
            "Config Import", 
            "MySQL Connector",
            "Analytics Database",
            "ML Database",
            "Data Loaders"
        ],
        default=[
            "Environment Variables",
            "Config Import",
            "MySQL Connector"
        ]
    )
    
    # Run tests
    if st.sidebar.button("▶️ Run Selected Tests") or st.session_state.get('run_tests', False):
        st.session_state.run_tests = False
        
        results = {}
        
        # Test 1: Environment Variables
        if "Environment Variables" in test_sections:
            results['env_vars'] = test_environment_variables()
            st.markdown("---")
        
        # Test 2: Config Import
        config_success = False
        config = None
        if "Config Import" in test_sections:
            config_success, config = test_config_import()
            results['config'] = config_success
            st.markdown("---")
        
        # Test 3: MySQL Connector
        if "MySQL Connector" in test_sections:
            results['mysql_connector'] = test_mysql_connector()
            st.markdown("---")
        
        # Test 4: Analytics Database
        if "Analytics Database" in test_sections and config_success:
            analytics_params = {
                'host': os.getenv("MYSQL_ANALYTICS_HOST"),
                'port': int(os.getenv("MYSQL_ANALYTICS_PORT")),
                'user': os.getenv("MYSQL_ANALYTICS_USER"),
                'password': os.getenv("MYSQL_ANALYTICS_PASSWORD"),
                'database': os.getenv("MYSQL_ANALYTICS_DATABASE")
            }
            results['analytics_db'] = test_database_connection("Analytics", analytics_params)
            st.markdown("---")
        
        # Test 5: ML Database
        if "ML Database" in test_sections and config_success:
            ml_params = {
                'host': os.getenv("MYSQL_ML_HOST"),
                'port': int(os.getenv("MYSQL_ML_PORT")),
                'user': os.getenv("MYSQL_ML_USER"),
                'password': os.getenv("MYSQL_ML_PASSWORD"),
                'database': os.getenv("MYSQL_ML_DATABASE")
            }
            results['ml_db'] = test_database_connection("ML", ml_params)
            st.markdown("---")
        
        # Test 6: Data Loaders
        if "Data Loaders" in test_sections:
            test_data_loaders()
            st.markdown("---")
        
        # Summary
        st.subheader("📋 Test Summary")
        
        total_tests = len(results)
        passed_tests = sum(1 for result in results.values() if result)
        
        if total_tests > 0:
            success_rate = (passed_tests / total_tests) * 100
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tests", total_tests)
            with col2:
                st.metric("Passed", passed_tests, f"{passed_tests - (total_tests - passed_tests)}")
            with col3:
                st.metric("Success Rate", f"{success_rate:.1f}%")
            
            # Detailed results
            for test_name, result in results.items():
                status = "✅ PASS" if result else "❌ FAIL"
                st.write(f"- {test_name.replace('_', ' ').title()}: {status}")
        
        if passed_tests == total_tests and total_tests > 0:
            st.success("🎉 All tests passed! Dashboard should work correctly.")
        elif passed_tests > 0:
            st.warning("⚠️ Some tests failed. Check the errors above.")
        else:
            st.error("❌ All tests failed. Please fix the configuration issues.")
    
    # Information section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Information")
    st.sidebar.info(f"Test run at: {datetime.now().strftime('%H:%M:%S')}")
    st.sidebar.info(f"Project root: {project_root}")

if __name__ == "__main__":
    main()