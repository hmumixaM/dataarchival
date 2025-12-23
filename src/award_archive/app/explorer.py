"""Streamlit app for exploring Delta Lake data."""

import os
import time

import duckdb
import streamlit as st


def get_db_connection():
    """Initialize DuckDB connection with Delta Lake support."""
    try:
        con = duckdb.connect(database=":memory:")

        # Install and load extensions
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL delta; LOAD delta;")

        # Configure AWS credentials if available
        if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
            region = os.environ.get('AWS_REGION', 'us-east-1')
            access_key = os.environ.get('AWS_ACCESS_KEY_ID')
            secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
            
            # Set S3 configuration for httpfs
            con.execute(f"SET s3_region='{region}';")
            con.execute(f"SET s3_access_key_id='{access_key}';")
            con.execute(f"SET s3_secret_access_key='{secret_key}';")
            
            # Create a secret for S3 access (used by delta extension)
            con.execute("DROP SECRET IF EXISTS aws_secret;")
            con.execute(f"""
                CREATE SECRET aws_secret (
                    TYPE S3,
                    KEY_ID '{access_key}',
                    SECRET '{secret_key}',
                    REGION '{region}'
                );
            """)
            
            if os.environ.get("AWS_SESSION_TOKEN"):
                con.execute(f"SET s3_session_token='{os.environ.get('AWS_SESSION_TOKEN')}';")

        return con
    except Exception as e:
        st.error(f"Failed to initialize DuckDB: {e}")
        return None


def run():
    """Run the Streamlit app."""
    # Page config
    st.set_page_config(page_title="Award Archive Explorer", layout="wide")

    st.title("Award Archive Explorer 🦆")

    # Sidebar - Configuration and Table List
    with st.sidebar:
        st.header("Settings")

        # Check for AWS credentials
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            st.warning("⚠️ AWS Credentials not found in environment variables.")
            st.info("Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
        else:
            st.success("✅ AWS Credentials detected")

        st.divider()

        st.header("Available Tables")
        st.markdown("- **availability**: `s3://award-archive/availability`")

        st.divider()
        st.markdown("### Info")
        st.markdown(
            """
        This explorer uses **DuckDB** running in-memory to query
        **Delta Lake** tables directly from S3.

        Use `delta_scan('s3://path')` to read tables.
        """
        )

    # Initialize DuckDB connection
    con = get_db_connection()

    if con:
        # Main Query Area
        st.subheader("SQL Query")

        default_query = "SELECT * FROM delta_scan('s3://award-archive/availability') LIMIT 10;"

        # Query Editor
        query = st.text_area(
            "Enter your DuckDB SQL query:",
            value=default_query,
            height=150,
            help="Write standard SQL. Use delta_scan('s3://path') to reference Delta tables.",
        )

        run_query = st.button("Run Query", type="primary")

        if run_query:
            if not query.strip():
                st.warning("Please enter a query.")
            else:
                try:
                    # Timer
                    start_time = time.time()

                    # Execute
                    df = con.sql(query).df()

                    end_time = time.time()
                    elapsed_time = end_time - start_time

                    # Metrics
                    st.success(f"Query executed in {elapsed_time:.4f} seconds")
                    st.markdown(f"**Rows returned:** {len(df)}")

                    # Results
                    st.dataframe(df, use_container_width=True)

                except Exception as e:
                    st.error(f"SQL Error: {e}")
    else:
        st.error("Could not establish database connection.")


if __name__ == "__main__":
    run()

