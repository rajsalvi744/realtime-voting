import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import simplejson as json
from confluent_kafka import Consumer
import streamlit as st
from streamlit_autorefresh import st_autorefresh


def create_kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'voting_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    return consumer


def fetch_data_from_kafka(consumer, topic_name):
    # Poll Kafka consumer for messages within a timeout period
    consumer.subscribe([topic_name])
    data = []
    try:
        for i in range(10):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                break
            else:
                value = json.loads(msg.value().decode('utf-8'))
                data.append(value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    return data


@st.cache_data()
def fetch_voting_status():
    conn = psycopg2.connect("host=localhost user=postgres password=postgres dbname=voting")
    cur = conn.cursor()

    cur.execute("""
    SELECT COUNT(*) AS voters_count from voters;
    """)
    voters_count = cur.fetchone()[0]
    cur.execute("""
    SELECT COUNT(*) as candidates_count from candidates;
    """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count


def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    viridis = plt.cm.get_cmap('viridis')
    colors = viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Counts per Candidate')
    plt.xticks(rotation=90)
    return plt


# Function to plot a donut chart for vote distribution
def plot_donut_chart(data: pd.DataFrame, title='Donut Chart', type='candidate'):
    if type == 'candidate':
        labels = list(data['candidate_name'])
    elif type == 'gender':
        labels = list(data['gender'])

    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title(title)
    return fig


# Function to plot a pie chart for vote distribution
def plot_pie_chart(data, title='Gender Distribution of Voters', labels=None):
    sizes = list(data.values())
    if labels is None:
        labels = list(data.keys())

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title(title)
    return fig


@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df[i:i + rows - 1] for i in range(0, len(input_df), rows)]
    return df


def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=['Yes', "No"], horizontal=1, index=1)
    if sort == 'Yes':
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=1)
        table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))

    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (int(len(table_data)) / batch_size if int(len(table_data)) / batch_size > 0 else 1)
        current_page = st.number_input("Page", min_value=1, max_value=int(total_pages), step=1)
    with bottom_menu[0]:
        st.markdown(f"Page**{current_page}**of**{total_pages}**")
    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last Refreshed at : {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_status()

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)
    consumer = create_kafka_consumer()
    data = fetch_data_from_kafka(consumer, topic_name='aggregated_votes_per_candidate')
    results = pd.DataFrame(data)

    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(f"Leading Candidate : {leading_candidate['candidate_name']}")
        st.subheader(f"Party Affiliation : {leading_candidate['party_affiliation']}")
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")

    st.markdown("""---""")
    st.header("Statistics")
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    col1, col2 = st.columns(2)

    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results, title='Votes Distribution')
        st.pyplot(donut_fig)

    st.table(results)

    location_consumer = create_kafka_consumer()
    location_data = fetch_data_from_kafka(location_consumer, topic_name='aggregated_turnout_by_location')
    location_result = pd.DataFrame(location_data)

    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    st.header("Location Of Voters")
    paginate_table(location_result)

    st.session_state['last_update'] = time.time()


def sidebar():
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh Interval (seconds):", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button('Refresh Data'):
        update_data()


st.title("Realtime Election Dashboard")
sidebar()
update_data()
