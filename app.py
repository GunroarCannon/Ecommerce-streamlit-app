
import streamlit as st
import sys



from dataloader import *
from schema import *
from model import *


import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, timedelta
import hashlib
import hmac
from sqlalchemy import create_engine
import numpy as np
import dill

# Page Configuration
st.set_page_config(
    page_title="🛍️ E-commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for advanced styling
def load_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Root variables for theming */
    :root {
        --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        --success-gradient: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        --warning-gradient: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
        --dark-bg: #0e1117;
        --card-bg: rgba(255, 255, 255, 0.05);
        --glass-bg: rgba(255, 255, 255, 0.1);
        --text-primary: #ffffff;
        --text-secondary: rgba(255, 255, 255, 0.7);
        --border-color: rgba(255, 255, 255, 0.1);
    }
    
    /* Global Styles */
    .main .block-container {
        padding-top: 2rem;
        max-width: 100%;
    }
    
    body {
        font-family: 'Inter', sans-serif;
        background: var(--dark-bg);
        color: var(--text-primary);
    }
    
    /* Grid background animation */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: 
            linear-gradient(90deg, rgba(255,255,255,0.02) 1px, transparent 1px),
            linear-gradient(rgba(255,255,255,0.02) 1px, transparent 1px);
        background-size: 50px 50px;
        animation: gridMove 20s linear infinite;
        pointer-events: none;
        z-index: -1;
    }
    
    @keyframes gridMove {
        0% { transform: translate(0, 0); }
        100% { transform: translate(50px, 50px); }
    }
    
    /* Header styling */
    .main-header {
        background: var(--primary-gradient);
        padding: 2rem 3rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 20px 40px rgba(102, 126, 234, 0.3);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .main-header h1 {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-shadow: 0 2px 10px rgba(0,0,0,0.3);
    }
    
    .main-header p {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        font-weight: 400;
        margin: 0;
    }
    
    /* Metric cards */
    .metric-card {
        background: var(--glass-bg);
        backdrop-filter: blur(20px);
        border: 1px solid var(--border-color);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: var(--primary-gradient);
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 15px 45px rgba(0, 0, 0, 0.4);
        border-color: rgba(255, 255, 255, 0.2);
    }
    
    .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 0.5rem;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: var(--text-secondary);
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .metric-change {
        font-size: 0.8rem;
        font-weight: 600;
        padding: 0.2rem 0.5rem;
        border-radius: 12px;
        margin-top: 0.5rem;
        display: inline-block;
    }
    
    .metric-change.positive {
        background: rgba(76, 175, 80, 0.2);
        color: #4CAF50;
        border: 1px solid rgba(76, 175, 80, 0.3);
    }
    
    .metric-change.negative {
        background: rgba(244, 67, 54, 0.2);
        color: #f44336;
        border: 1px solid rgba(244, 67, 54, 0.3);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: var(--glass-bg);
        backdrop-filter: blur(20px);
        border-right: 1px solid var(--border-color);
    }
    
    /* Filter containers */
    .filter-container {
        background: var(--card-bg);
        backdrop-filter: blur(10px);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    /* Chart containers */
    .chart-container {
        background: var(--card-bg);
        backdrop-filter: blur(10px);
        border: 1px solid var(--border-color);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    }
    
    /* Custom button styling */
    .stButton > button {
        background: var(--primary-gradient);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.4);
    }
    
    /* Login form styling */
    .login-form {
        max-width: 400px;
        margin: 0 auto;
        padding: 2rem;
        background: var(--glass-bg);
        backdrop-filter: blur(20px);
        border: 1px solid var(--border-color);
        border-radius: 20px;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
    }
    
    .login-header {
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .login-header h2 {
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    /* Success/Error alerts */
    .alert {
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
    }
    
    .alert-success {
        background: rgba(76, 175, 80, 0.1);
        border: 1px solid rgba(76, 175, 80, 0.3);
        color: #4CAF50;
    }
    
    .alert-error {
        background: rgba(244, 67, 54, 0.1);
        border: 1px solid rgba(244, 67, 54, 0.3);
        color: #f44336;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header {
            padding: 1.5rem;
        }
        
        .main-header h1 {
            font-size: 2rem;
        }
        
        .metric-value {
            font-size: 1.8rem;
        }
    }
    
    /* Loading animation */
    .loading-spinner {
        width: 40px;
        height: 40px;
        border: 4px solid rgba(102, 126, 234, 0.3);
        border-top: 4px solid #667eea;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin: 20px auto;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    </style>
    """, unsafe_allow_html=True)

# Authentication functions
def hash_password(password):
    """Hash a password for storing."""
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_password(stored_password, provided_password):
    """Verify a stored password against provided password."""
    return stored_password == hash_password(provided_password)

def init_auth():
    """Initialize authentication state."""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = ""

# User database (in production, use a proper database)
USERS = {
    "admin": hash_password("admin123"),
    "analyst": hash_password("analyst123"),
    "manager": hash_password("manager123"),
    "demo": hash_password("demo123")
}

def login_page():
    """Display login page."""
    st.markdown('<div class="login-form">', unsafe_allow_html=True)
    
    st.markdown("""
        <div class="login-header">
            <h2>🛍️ E-commerce Analytics</h2>
            <p style="color: rgba(255,255,255,0.7);">Sign in to access your dashboard</p>
        </div>
    """, unsafe_allow_html=True)
    
    with st.form("login_form"):
        username = st.text_input("Username", placeholder="Enter your username")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        submit_button = st.form_submit_button("🔐 Sign In", use_container_width=True)
        
        if submit_button:
            if username in USERS and check_password(USERS[username], password):
                st.session_state.authenticated = True
                st.session_state.username = username
                st.success("✅ Login successful!")
                st.rerun()
            else:
                st.error("❌ Invalid username or password")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Demo credentials
    st.markdown("""
        <div style="margin-top: 2rem; padding: 1rem; background: rgba(255,255,255,0.05); border-radius: 12px;">
            <h4 style="margin-bottom: 1rem;">📝 Demo Credentials:</h4>
            <div style="font-family: monospace; font-size: 0.9rem;">
                <strong>Username:</strong> demo<br>
                <strong>Password:</strong> demo123
            </div>
        </div>
    """, unsafe_allow_html=True)

# Initialize your database connection and classes here
@st.cache_resource
def init_database():
    """Initialize database connection and ETL classes."""
    # Replace with your actual database initialization
    try:
        # warehouse = EcommerceDimensionalModel()
        # etl = EcommerceETL(warehouse)
           # Import the already-created objects from model.py
           from model import warehouse, etl, queries
           return warehouse, etl, queries
    except Exception as e:
        st.error(f"Database initialization failed: {e}")
        return None, None, None

# Sample data for demonstration (replace with your actual data)
@st.cache_data
def load_sample_data():
    try:
        # Get your actual data from queries
        warehouse, etl, queries = init_database()
        if queries is None:
            raise Exception("Database not initialized")
        
        # Get data from your SQL queries
        monthly_data = queries.monthly_sales_trend()
        country_data = queries.top_sales_by_country()
        product_data = queries.top_n_products(20)
        
        # Map columns for sales_data (expected: date, revenue, orders, customers)
        sales_data = monthly_data.copy()
        if 'year_month' in sales_data.columns:
            # Convert year_month (202301) to proper date
            sales_data['date'] = pd.to_datetime(sales_data['year_month'], format='%Y%m')
        # revenue, orders, customers columns should already exist from your query
        
        # Map columns for country_data (expected: country, revenue, customers)
        # Your query returns: country, revenue
        # Add dummy customers column if missing
        if 'customers' not in country_data.columns:
            country_data['customers'] = country_data['revenue'] / 100  # dummy calculation
        
        # Map columns for product_data (expected: product, revenue, quantity)
        # Your query returns: description, revenue
        # Rename and add missing columns
        product_data = product_data.rename(columns={'description': 'product'})
        if 'quantity' not in product_data.columns:
            product_data['quantity'] = product_data['revenue'] / 50  # dummy calculation
        
        return sales_data, country_data, product_data
        
    except Exception as e:
        st.error(f"Error loading data from queries: {e}")
        # Return empty DataFrames as fallback
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
def create_metric_card(title, value, change=None, format_func=None):
    """Create a styled metric card."""
    if format_func:
        formatted_value = format_func(value)
    else:
        formatted_value = f"{value:,.0f}" if isinstance(value, (int, float)) else str(value)
    
    change_html = ""
    if change is not None:
        change_class = "positive" if change >= 0 else "negative"
        change_symbol = "↗️" if change >= 0 else "↘️"
        change_html = f'<div class="metric-change {change_class}">{change_symbol} {abs(change):.1f}%</div>'
    
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{formatted_value}</div>
            <div class="metric-label">{title}</div>
            {change_html}
        </div>
    """, unsafe_allow_html=True)

def create_charts(sales_data, country_data, product_data, date_range, selected_countries):
    """Create all dashboard charts."""
    
    try:
        # Convert date_range to datetime for comparison
        if len(date_range) == 2:
            start_date = pd.to_datetime(date_range[0])
            end_date = pd.to_datetime(date_range[1])
            # Filter data based on selections
            filtered_sales = sales_data[
                (sales_data['date'] >= start_date) & 
                (sales_data['date'] <= end_date)
            ]
        else:
            filtered_sales = sales_data
        
        if filtered_sales.empty:
            st.warning("No data available for the selected date range.")
            return None, None, None, None
        
        filtered_countries = country_data[country_data['country'].isin(selected_countries)]
        
        # 1. Sales Trend Chart
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=filtered_sales['date'],
            y=filtered_sales['revenue'],
            mode='lines',
            name='Daily Revenue',
            line=dict(color='#667eea', width=3),
            fill='tonexty',
            fillcolor='rgba(102, 126, 234, 0.1)'
        ))
        
        fig_trend.update_layout(
            title="📈 Sales Trend Over Time",
            xaxis_title="Date",
            yaxis_title="Revenue ($)",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            title_font_size=16,
            showlegend=False,
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
        )
        
        # 2. Revenue by Country
        fig_country = px.bar(
            filtered_countries.head(10),
            x='country',
            y='revenue',
            title="🌍 Revenue by Country (Top 10)",
            color='revenue',
            color_continuous_scale='viridis'
        )
        fig_country.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            title_font_size=16,
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)')
        )
        
        # 3. Top Products
        fig_products = px.pie(
            product_data.head(8),
            values='revenue',
            names='product',
            title="🏆 Top Products by Revenue",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_products.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            title_font_size=16,
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.01
            )
        )
        
        # 4. Monthly Revenue Heatmap
        monthly_data = filtered_sales.copy()
        monthly_data['month'] = monthly_data['date'].dt.month
        monthly_data['year'] = monthly_data['date'].dt.year
        monthly_pivot = monthly_data.groupby(['year', 'month'])['revenue'].sum().unstack(fill_value=0)
        
        if not monthly_pivot.empty:
            fig_heatmap = px.imshow(
                monthly_pivot.values,
                x=[f"M{i}" for i in range(1, 13)],
                y=monthly_pivot.index,
                title="🔥 Revenue Heatmap (Monthly)",
                color_continuous_scale='viridis',
                aspect='auto'
            )
            fig_heatmap.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                title_font_size=16,
                xaxis_title="Month",
                yaxis_title="Year"
            )
        else:
            # Create empty heatmap if no data
            fig_heatmap = go.Figure()
            fig_heatmap.update_layout(
                title="🔥 Revenue Heatmap (Monthly) - No Data",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white'),
                title_font_size=16
            )
        
        return fig_trend, fig_country, fig_products, fig_heatmap
    
    except Exception as e:
        st.error(f"Error creating charts: {e}")
        return None, None, None, None

def main_dashboard():
    """Main dashboard interface."""
    
    # Header
    st.markdown(f"""
        <div class="main-header">
            <h1>🛍️ E-commerce Analytics Dashboard</h1>
            <p>Welcome back, <strong>{st.session_state.username}</strong> • Real-time insights into your business performance</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Initialize database (replace with your actual implementation)
    warehouse, etl, queries = init_database()
    
    # Load sample data for demo
    sales_data, country_data, product_data = load_sample_data()
    
    # Sidebar filters
    with st.sidebar:
        st.markdown("### 🎛️ Dashboard Filters")
        
        # Date range filter
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        st.subheader("📅 Date Range")
        date_range = st.date_input(
            "Select date range",
            value=(datetime(2010, 1, 1), datetime(2011, 12, 31)),
            min_value=datetime(2010, 1, 1),
            max_value=datetime(2011, 12, 31)
        )
        
        # Ensure date_range is always a tuple
        if not isinstance(date_range, tuple):
            if hasattr(date_range, '__iter__') and len(date_range) == 2:
                date_range = tuple(date_range)
            else:
                date_range = (datetime(2024, 1, 1), datetime(2024, 12, 31))
        elif len(date_range) == 1:
            date_range = (date_range[0], date_range[0])
            
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Country filter
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        st.subheader("🌍 Countries")
        available_countries = country_data['country'].tolist()
        selected_countries = st.multiselect(
            "Select countries",
            available_countries,
            default=available_countries[:5]
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = ""
            st.rerun()
    
    # Key Metrics Row
    st.markdown("### 📊 Key Performance Indicators")
    
    # Filter sales data for metrics
    # In main_dashboard(), replace the date filtering section with:
    if len(date_range) == 2:
        start_date = pd.to_datetime(date_range[0])
        end_date = pd.to_datetime(date_range[1])
        
        # Convert year_month to datetime for comparison
        if 'year_month' in sales_data.columns:
            sales_data['date'] = pd.to_datetime(sales_data['year_month'], format='%Y%m')
        
        filtered_sales = sales_data[
            (sales_data['date'] >= start_date) & 
            (sales_data['date'] <= end_date)
        ]
        print(sales_data.head(10))
        print(start_date, end_date)
        print(filtered_sales.head())
    else:
        filtered_sales = sales_data
        print("date range not 2", filtered_sales.head())
    
    print("here we go ", filtered_sales.head(3))
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = filtered_sales['revenue'].sum()
        create_metric_card("Total Revenue", total_revenue, change=12.5, format_func=lambda x: f"${x:,.0f}")
    
    with col2:
        total_orders = filtered_sales['orders'].sum()
        create_metric_card("Total Orders", total_orders, change=8.3)
    
    with col3:
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        create_metric_card("Avg Order Value", avg_order_value, change=-2.1, format_func=lambda x: f"${x:.0f}")
    
    with col4:
        total_customers = filtered_sales['customers'].sum()
        create_metric_card("Total Customers", total_customers, change=15.7)
    
    # Charts Section
    if selected_countries:
        charts = create_charts(sales_data, country_data, product_data, date_range, selected_countries)
        
        if all(chart is not None for chart in charts):
            fig_trend, fig_country, fig_products, fig_heatmap = charts
            
            # First row of charts
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_trend, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_products, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Second row of charts
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_country, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_heatmap, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.error("❌ Unable to generate charts. Please check your data and try again.")
        
        # Data Tables Section
        st.markdown("### 📋 Detailed Analytics")
        
        tab1, tab2, tab3 = st.tabs(["🏆 Top Products", "🌍 Country Performance", "📈 Sales Data"])
        
        with tab1:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.dataframe(
                product_data.head(10).style.format({
                    'revenue': '${:,.0f}',
                    'quantity': '{:,}'
                }),
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            filtered_country_display = country_data[country_data['country'].isin(selected_countries)]
            st.dataframe(
                filtered_country_display.style.format({
                    'revenue': '${:,.0f}',
                    'customers': '{:,}'
                }),
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab3:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.dataframe(
                filtered_sales.tail(20).style.format({
                    'revenue': '${:,.0f}',
                    'orders': '{:,}',
                    'customers': '{:,}'
                }),
                use_container_width=True
            )
            st.markdown('</div>', unsafe_allow_html=True)
    
    else:
        st.warning("⚠️ Please select at least one country to display charts.")

def main():
    """Main application entry point."""
    
    # Load custom CSS
    load_css()
    
    # Initialize authentication
    init_auth()
    
    # Check authentication
    if not st.session_state.authenticated:
        login_page()
    else:
        main_dashboard()

if __name__ == "__main__":
    main()