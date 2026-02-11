import streamlit as st 
import pandas as pd 
from ydata_profiling import ProfileReport
from streamlit_pandas_profiling import st_profile_report
import sys
import os
from datetime import datetime
from time_management_agent import generate_time_management_excel
st.set_page_config(page_title='Data Profiler & Time Management',layout='wide')


def get_filesize(file):
    size_bytes = sys.getsizeof(file)
    size_mb = size_bytes/(1024**2)
    return size_mb

def validate_file(file):
    filename = file.name
    name, ext = os.path.splitext(filename)
    if ext in ('.csv','.xlsx'):
        return ext
    else:
        return False
    

# sidebar
with st.sidebar:
    st.header("Navigation")
    app_mode = st.radio("Select Mode:", 
                        options=('Data Profiler', 'Time Management Excel Generator'))
    
    if app_mode == 'Data Profiler':
        uploaded_file = st.file_uploader("Upload .csv, .xlsx files not exceeding 10 MB")
        if uploaded_file is not None:
            st.write('Modes of Operation')
            minimal = st.checkbox('Do you want minimal report ?')
            display_mode = st.radio('Display mode:',
                                    options=('Primary','Dark','Orange'))
            if display_mode == 'Dark':
                dark_mode= True
                orange_mode = False
            elif display_mode == 'Orange':
                dark_mode = False
                orange_mode = True
            else:
                dark_mode = False
                orange_mode = False
    else:
        uploaded_file = None
        minimal = False
        dark_mode = False
        orange_mode = False
        
    
if app_mode == 'Data Profiler':
    if uploaded_file is not None:
        ext = validate_file(uploaded_file)
        if ext:
            filesize = get_filesize(uploaded_file)
            if filesize <=10:
            
                if ext == '.csv':
                    # time being let load csv
                    df = pd.read_csv(uploaded_file)
                else:
                    xl_file = pd.ExcelFile(uploaded_file)
                    sheet_tuple = tuple(xl_file.sheet_names)
                    sheet_name = st.sidebar.selectbox('Select the sheet',sheet_tuple)
                    df = xl_file.parse(sheet_name)
                    
                    
                # generate report
                with st.spinner('Generating Report'):
                    pr = ProfileReport(df,
                                    minimal=minimal,
                                    dark_mode=dark_mode,
                                    orange_mode=orange_mode
                                    )
                    
                st_profile_report(pr)
            else:
                st.error(f'Maximum allowed filesize is 10 MB.But received {filesize} MB')
                
        else:
            st.error('Kindly upload only .csv or .xlsx file')
    else:
        st.title('Data Profiler')
        st.info('Upload your data in the left sidebar to generate profiling')

elif app_mode == 'Time Management Excel Generator':
    st.title('📅 Time Management Excel Generator')
    st.write('Generate customized time management Excel templates to help you organize your tasks and schedule.')
    
    # Template selection
    col1, col2 = st.columns(2)
    
    with col1:
        template_type = st.selectbox(
            'Select Template Type:',
            options=['Daily Schedule', 'Weekly Plan', 'Monthly Plan', 'Task Tracker'],
            help='Choose the type of time management template you need'
        )
    
    with col2:
        start_date = st.date_input(
            'Start Date:',
            value=datetime.now(),
            help='Select the starting date for your template'
        )
    
    # Template-specific options
    if template_type == 'Daily Schedule':
        num_days = st.slider('Number of Days:', min_value=1, max_value=30, value=7)
        template_key = 'daily'
        kwargs = {'start_date': datetime.combine(start_date, datetime.min.time()), 'num_days': num_days}
        
        st.info('📋 Daily Schedule template includes time slots from 6 AM to 10 PM in 30-minute intervals.')
        
    elif template_type == 'Weekly Plan':
        num_weeks = st.slider('Number of Weeks:', min_value=1, max_value=12, value=4)
        template_key = 'weekly'
        kwargs = {'start_date': datetime.combine(start_date, datetime.min.time()), 'num_weeks': num_weeks}
        
        st.info('📋 Weekly Plan template includes a breakdown by days with task priorities and status tracking.')
        
    elif template_type == 'Monthly Plan':
        num_months = st.slider('Number of Months:', min_value=1, max_value=12, value=3)
        template_key = 'monthly'
        kwargs = {'start_date': datetime.combine(start_date, datetime.min.time()), 'num_months': num_months}
        
        st.info('📋 Monthly Plan template includes goals, categories, priorities, and progress tracking.')
        
    else:  # Task Tracker
        template_key = 'task_tracker'
        kwargs = {}
        
        st.info('📋 Task Tracker template includes 20 pre-formatted tasks with priority levels, status tracking, and time estimates.')
    
    # Generate button
    if st.button('🚀 Generate Excel Template', type='primary'):
        try:
            with st.spinner('Generating your time management Excel template...'):
                # Generate the Excel file
                excel_file = generate_time_management_excel(template_key, **kwargs)
                
                # Create download button
                file_name = f"{template_type.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                
                st.success('✅ Template generated successfully!')
                
                st.download_button(
                    label='📥 Download Excel Template',
                    data=excel_file,
                    file_name=file_name,
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                st.balloons()
                
        except Exception as e:
            st.error(f'❌ An error occurred while generating the template: {str(e)}')
    
    # Display features
    st.markdown('---')
    st.subheader('✨ Features')
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📅 Daily Schedule**
        - Time slots every 30 min
        - Multi-day planning
        - Professional styling
        """)
    
    with col2:
        st.markdown("""
        **📊 Weekly Plan**
        - Week-by-week breakdown
        - Priority tracking
        - Status updates
        """)
    
    with col3:
        st.markdown("""
        **📈 Monthly Plan**
        - Long-term planning
        - Goal categorization
        - Progress monitoring
        """)
    
    st.markdown('---')
    st.markdown("""
    **💡 Tips for Using Your Templates:**
    - Download the template and open it in Microsoft Excel or Google Sheets
    - All templates include professional formatting and frozen headers
    - Task Tracker includes data validation for priority and status fields
    - Customize colors and add more rows as needed
    """)
      

    