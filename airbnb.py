import pandas as pd
import pymongo
import psycopg2
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
pd.set_option('display.max_columns', None)


def streamlit_config():

    # page configuration
    page_icon_url = 'https://raw.githubusercontent.com/gopiashokan/Airbnb-Analysis/main/airbnb_logo.png'
    st.set_page_config(page_title='Airbnb',
                       page_icon=page_icon_url, layout="wide")

    # page header transparent color
    page_background_color = """
    <style>

    [data-testid="stHeader"] 
    {
    background: rgba(0,0,0,0);
    }

    </style>
    """
    st.markdown(page_background_color, unsafe_allow_html=True)

    # title and position
    st.markdown(f'<h1 style="text-align: center;">Airbnb Analysis</h1>',
                unsafe_allow_html=True)


class data_collection:
    gopi = pymongo.MongoClient("mongodb://gopiashokan:gopiroot@ac-0vdscni-shard-00-00.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-01.xdp3lkp.mongodb.net:27017,ac-0vdscni-shard-00-02.xdp3lkp.mongodb.net:27017/?ssl=true&replicaSet=atlas-11e4qv-shard-0&authSource=admin&retryWrites=true&w=majority")
    db = gopi['sample_airbnb']
    col = db['listingsAndReviews']


class data_preprocessing:

    def primary():
        # direct feature columns
        data = []
        for i in data_collection.col.find({}, {'_id': 1, 'listing_url': 1, 'name': 1, 'property_type': 1, 'room_type': 1, 'bed_type': 1,
                                               'minimum_nights': 1, 'maximum_nights': 1, 'cancellation_policy': 1, 'accommodates': 1,
                                               'bedrooms': 1, 'beds': 1, 'number_of_reviews': 1, 'bathrooms': 1, 'price': 1,
                                               'cleaning_fee': 1, 'extra_people': 1, 'guests_included': 1, 'images.picture_url': 1,
                                               'review_scores.review_scores_rating': 1}):
            data.append(i)

        df_1 = pd.DataFrame(data)
        df_1['images'] = df_1['images'].apply(lambda x: x['picture_url'])
        df_1['review_scores'] = df_1['review_scores'].apply(
            lambda x: x.get('review_scores_rating', 0))

        # null value handling
        df_1['bedrooms'].fillna(0, inplace=True)
        df_1['beds'].fillna(0, inplace=True)
        df_1['bathrooms'].fillna(0, inplace=True)
        df_1['cleaning_fee'].fillna('Not Specified', inplace=True)

        # data types conversion
        df_1['minimum_nights'] = df_1['minimum_nights'].astype(int)
        df_1['maximum_nights'] = df_1['maximum_nights'].astype(int)
        df_1['bedrooms'] = df_1['bedrooms'].astype(int)
        df_1['beds'] = df_1['beds'].astype(int)
        df_1['bathrooms'] = df_1['bathrooms'].astype(str).astype(float)
        df_1['price'] = df_1['price'].astype(str).astype(float).astype(int)
        df_1['cleaning_fee'] = df_1['cleaning_fee'].apply(lambda x: int(
            float(str(x))) if x != 'Not Specified' else 'Not Specified')
        df_1['extra_people'] = df_1['extra_people'].astype(
            str).astype(float).astype(int)
        df_1['guests_included'] = df_1['guests_included'].astype(
            str).astype(int)

        return df_1

    def host():
        host = []
        for i in data_collection.col.find({}, {'_id': 1, 'host': 1}):
            host.append(i)

        df_host = pd.DataFrame(host)
        host_keys = list(df_host.iloc[0, 1].keys())
        host_keys.remove('host_about')

        # make nested dictionary to separate columns
        for i in host_keys:
            if i == 'host_response_time':
                df_host['host_response_time'] = df_host['host'].apply(
                    lambda x: x['host_response_time'] if 'host_response_time' in x else 'Not Specified')
            else:
                df_host[i] = df_host['host'].apply(
                    lambda x: x[i] if i in x and x[i] != '' else 'Not Specified')

        df_host.drop(columns=['host'], inplace=True)

        # data type conversion
        df_host['host_is_superhost'] = df_host['host_is_superhost'].map(
            {False: 'No', True: 'Yes'})
        df_host['host_has_profile_pic'] = df_host['host_has_profile_pic'].map(
            {False: 'No', True: 'Yes'})
        df_host['host_identity_verified'] = df_host['host_identity_verified'].map(
            {False: 'No', True: 'Yes'})

        return df_host

    def address():
        address = []
        for i in data_collection.col.find({}, {'_id': 1, 'address': 1}):
            address.append(i)

        df_address = pd.DataFrame(address)
        address_keys = list(df_address.iloc[0, 1].keys())

        # nested dicionary to separate columns
        for i in address_keys:
            if i == 'location':
                df_address['location_type'] = df_address['address'].apply(
                    lambda x: x['location']['type'])
                df_address['longitude'] = df_address['address'].apply(
                    lambda x: x['location']['coordinates'][0])
                df_address['latitude'] = df_address['address'].apply(
                    lambda x: x['location']['coordinates'][1])
                df_address['is_location_exact'] = df_address['address'].apply(
                    lambda x: x['location']['is_location_exact'])
            else:
                df_address[i] = df_address['address'].apply(
                    lambda x: x[i] if x[i] != '' else 'Not Specified')

        df_address.drop(columns=['address'], inplace=True)

        # bool data conversion to string
        df_address['is_location_exact'] = df_address['is_location_exact'].map(
            {False: 'No', True: 'Yes'})
        return df_address

    def availability():
        availability = []
        for i in data_collection.col.find({}, {'_id': 1, 'availability': 1}):
            availability.append(i)

        df_availability = pd.DataFrame(availability)
        availability_keys = list(df_availability.iloc[0, 1].keys())

        # nested dicionary to separate columns
        for i in availability_keys:
            df_availability['availability_30'] = df_availability['availability'].apply(
                lambda x: x['availability_30'])
            df_availability['availability_60'] = df_availability['availability'].apply(
                lambda x: x['availability_60'])
            df_availability['availability_90'] = df_availability['availability'].apply(
                lambda x: x['availability_90'])
            df_availability['availability_365'] = df_availability['availability'].apply(
                lambda x: x['availability_365'])

        df_availability.drop(columns=['availability'], inplace=True)
        return df_availability

    def amenities_sort(x):
        a = x
        a.sort(reverse=False)
        return a

    def amenities():
        amenities = []
        for i in data_collection.col.find({}, {'_id': 1, 'amenities': 1}):
            amenities.append(i)

        df_amenities = pd.DataFrame(amenities)

        # sort the list of amenities
        df_amenities['amenities'] = df_amenities['amenities'].apply(
            lambda x: data_preprocessing.amenities_sort(x))
        return df_amenities

    def merge_dataframe():
        df_1 = data_preprocessing.primary()
        df_host = data_preprocessing.host()
        df_address = data_preprocessing.address()
        df_availability = data_preprocessing.availability()
        df_amenities = data_preprocessing.amenities()

        df = pd.merge(df_1, df_host, on='_id')
        df = pd.merge(df, df_address, on='_id')
        df = pd.merge(df, df_availability, on='_id')
        df = pd.merge(df, df_amenities, on='_id')

        return df


class sql:

    def create_table():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""create table if not exists airbnb(
                            _id					varchar(255) primary key,
                            listing_url			text,
                            name				varchar(255),
                            property_type		varchar(255),
                            room_type			varchar(255),
                            bed_type			varchar(255),
                            minimum_nights		int,
                            maximum_nights		int,
                            cancellation_policy	varchar(255),
                            accommodates		int,
                            bedrooms			int,
                            beds				int,
                            number_of_reviews	int,
                            bathrooms			float,
                            price				int,
                            cleaning_fee		varchar(20),
                            extra_people		int,
                            guests_included		int,
                            images				text,
                            review_scores		int,
                            host_id				varchar(255),
                            host_url			text,
                            host_name			varchar(255),
                            host_location		varchar(255),
                            host_response_time			varchar(255),
                            host_thumbnail_url			text,
                            host_picture_url			text,
                            host_neighbourhood			varchar(255),
                            host_response_rate			varchar(255),
                            host_is_superhost			varchar(25),
                            host_has_profile_pic		varchar(25),
                            host_identity_verified		varchar(25),
                            host_listings_count			int,
                            host_total_listings_count	int,
                            host_verifications			text,
                            street				varchar(255),
                            suburb				varchar(255),
                            government_area		varchar(255),
                            market				varchar(255),
                            country				varchar(255),
                            country_code		varchar(255),
                            location_type		varchar(255),
                            longitude			float,
                            latitude			float,
                            is_location_exact	varchar(25),
                            availability_30		int,
                            availability_60		int,
                            availability_90		int,
                            availability_365	int,
                            amenities			text);""")

        gopi.commit()
        gopi.close()

    def data_migration():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        df = data_preprocessing.merge_dataframe()

        cursor.executemany("insert into airbnb \
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", df.values.tolist())
        gopi.commit()
        gopi.close()

    def delete_table():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""delete from airbnb;""")
        gopi.commit()
        gopi.close()


class plotly:

    def pie_chart(df, x, y, title, title_x=0.20):

        fig = px.pie(df, names=x, values=y, hole=0.5, title=title)

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(text=df[y], textinfo='percent+value',
                          textposition='outside',
                          textfont=dict(color='white'))

        st.plotly_chart(fig, use_container_width=True)

    def horizontal_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[x]) * 0.75 else 'outside' for val in df[x]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True)

    def vertical_bar_chart(df, x, y, text, color, title, title_x=0.25):

        fig = px.bar(df, x=x, y=y, labels={x: '', y: ''}, title=title)

        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=False)

        fig.update_layout(title_x=title_x, title_font_size=22)

        text_position = ['inside' if val >= max(
            df[y]) * 0.90 else 'outside' for val in df[y]]

        fig.update_traces(marker_color=color,
                          text=df[text],
                          textposition=text_position,
                          texttemplate='%{y}<br>%{text}',
                          textfont=dict(size=14),
                          insidetextfont=dict(color='white'),
                          textangle=0,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)

    def line_chart(df, x, y, text, textposition, color, title, title_x=0.25):

        fig = px.line(df, x=x, y=y, labels={
                      x: '', y: ''}, title=title, text=df[text])

        fig.update_layout(title_x=title_x, title_font_size=22)

        fig.update_traces(line=dict(color=color, width=3.5),
                          marker=dict(symbol='diamond', size=10),
                          texttemplate='%{x}<br>%{text}',
                          textfont=dict(size=13.5),
                          textposition=textposition,
                          hovertemplate='%{x}<br>%{y}')

        st.plotly_chart(fig, use_container_width=True, height=100)


class feature:

    def feature(column_name, order='count desc', limit=10):
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select distinct {column_name}, count({column_name}) as count
                           from airbnb
                           group by {column_name}
                           order by {order}
                           limit {limit};""")
        gopi.commit()
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=[column_name, 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data[column_name].apply(lambda x: str(x)+'`')
        return data

    def cleaning_fee():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select distinct cleaning_fee, count(cleaning_fee) as count
                           from airbnb
                           where cleaning_fee != 'Not Specified'
                           group by cleaning_fee
                           order by count desc
                           limit 10;""")
        gopi.commit()
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=['cleaning_fee', 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data['cleaning_fee'].apply(lambda x: str(x)+'`')
        return data

    def location():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select host_id, country, longitude, latitude
                           from airbnb
                           group by host_id, country, longitude, latitude""")
        gopi.commit()
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(
            s, columns=['Host ID', 'Country', 'Longitude', 'Latitude'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def feature_analysis():

        # vertical_bar chart
        property_type = feature.feature('property_type')
        plotly.vertical_bar_chart(df=property_type, x='property_type', y='count',
                                  text='percentage', color='#5D9A96', title='Property Type', title_x=0.43)

        # line & pie chart
        col1, col2 = st.columns(2)
        with col1:
            bed_type = feature.feature('bed_type')
            plotly.line_chart(df=bed_type, y='bed_type', x='count', text='percentage', color='#5cb85c',
                              textposition=[
                                  'top center', 'bottom center', 'middle right', 'middle right', 'middle right'],
                              title='Bed Type', title_x=0.50)
        with col2:
            room_type = feature.feature('room_type')
            plotly.pie_chart(df=room_type, x='room_type',
                             y='count', title='Room Type', title_x=0.30)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Minimum Nights', 'Maximum Nights'])
        with tab1:
            minimum_nights = feature.feature('minimum_nights')
            plotly.vertical_bar_chart(df=minimum_nights, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Minimum Nights', title_x=0.43)
        with tab2:
            maximum_nights = feature.feature('maximum_nights')
            plotly.vertical_bar_chart(df=maximum_nights, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Maximum Nights', title_x=0.43)

        # line chart
        cancellation_policy = feature.feature('cancellation_policy')
        plotly.line_chart(df=cancellation_policy, y='cancellation_policy', x='count', text='percentage', color='#5D9A96',
                          textposition=['top center', 'top right',
                                        'top center', 'bottom center', 'middle right'],
                          title='Cancellation Policy', title_x=0.43)

        # vertical_bar chart
        accommodates = feature.feature('accommodates')
        plotly.vertical_bar_chart(df=accommodates, x='y', y='count', text='percentage',
                                  color='#5D9A96', title='Accommodates', title_x=0.43)

        # vertical_bar chart
        tab1, tab2, tab3 = st.tabs(['Bedrooms', 'Beds', 'Bathrooms'])
        with tab1:
            bedrooms = feature.feature('bedrooms')
            plotly.vertical_bar_chart(df=bedrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bedrooms', title_x=0.43)
        with tab2:
            beds = feature.feature('beds')
            plotly.vertical_bar_chart(df=beds, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Beds', title_x=0.43)
        with tab3:
            bathrooms = feature.feature('bathrooms')
            plotly.vertical_bar_chart(df=bathrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bathrooms', title_x=0.43)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(
            ['Price', 'Cleaning Fee', 'Extra People', 'Guests Included'])
        with tab1:
            price = feature.feature('price')
            plotly.vertical_bar_chart(df=price, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Price', title_x=0.43)
        with tab2:
            cleaning_fee = feature.cleaning_fee()
            plotly.vertical_bar_chart(df=cleaning_fee, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Cleaning Fee', title_x=0.43)
        with tab3:
            extra_people = feature.feature('extra_people')
            plotly.vertical_bar_chart(df=extra_people, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Extra People', title_x=0.43)
        with tab4:
            guests_included = feature.feature('guests_included')
            plotly.vertical_bar_chart(df=guests_included, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Guests Included', title_x=0.43)

        # line chart
        host_response_time = feature.feature('host_response_time')
        plotly.line_chart(df=host_response_time, y='host_response_time', x='count', text='percentage', color='#5cb85c',
                          textposition=['top center', 'top right',
                                        'top right', 'bottom left', 'bottom left'],
                          title='Host Response Time', title_x=0.43)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Host Response Rate', 'Host Listings Count'])
        with tab1:
            host_response_rate = feature.feature('host_response_rate')
            plotly.vertical_bar_chart(df=host_response_rate, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Host Response Rate', title_x=0.43)
        with tab2:
            host_listings_count = feature.feature('host_listings_count')
            plotly.vertical_bar_chart(df=host_listings_count, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Host Listings Count', title_x=0.43)

        # pie chart
        tab1, tab2, tab3 = st.tabs(
            ['Host is Superhost', 'Host has Profile Picture', 'Host Identity Verified'])
        with tab1:
            host_is_superhost = feature.feature('host_is_superhost')
            plotly.pie_chart(df=host_is_superhost, x='host_is_superhost',
                             y='count', title='Host is Superhost', title_x=0.39)
        with tab2:
            host_has_profile_pic = feature.feature('host_has_profile_pic')
            plotly.pie_chart(df=host_has_profile_pic, x='host_has_profile_pic',
                             y='count', title='Host has Profile Picture', title_x=0.37)
        with tab3:
            host_identity_verified = feature.feature('host_identity_verified')
            plotly.pie_chart(df=host_identity_verified, x='host_identity_verified',
                             y='count', title='Host Identity Verified', title_x=0.37)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3 = st.tabs(['Market', 'Country', 'Location Exact'])
        with tab1:
            market = feature.feature('market', limit=12)
            plotly.vertical_bar_chart(df=market, x='market', y='count', text='percentage',
                                      color='#5D9A96', title='Market', title_x=0.43)
        with tab2:
            country = feature.feature('country')
            plotly.vertical_bar_chart(df=country, x='country', y='count', text='percentage',
                                      color='#5D9A96', title='Country', title_x=0.43)
        with tab3:
            is_location_exact = feature.feature('is_location_exact')
            plotly.pie_chart(df=is_location_exact, x='is_location_exact', y='count',
                             title='Location Exact', title_x=0.37)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3, tab4 = st.tabs(['Availability 30', 'Availability 60',
                                          'Availability 90', 'Availability 365'])
        with tab1:
            availability_30 = feature.feature('availability_30')
            plotly.vertical_bar_chart(df=availability_30, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 30', title_x=0.45)
        with tab2:
            availability_60 = feature.feature('availability_60')
            plotly.vertical_bar_chart(df=availability_60, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 60', title_x=0.45)
        with tab3:
            availability_90 = feature.feature('availability_90')
            plotly.vertical_bar_chart(df=availability_90, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 90', title_x=0.45)
        with tab4:
            availability_365 = feature.feature('availability_365')
            plotly.vertical_bar_chart(df=availability_365, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability 365', title_x=0.45)

        # vertical_bar,pie,map chart
        tab1, tab2, tab3 = st.tabs(
            ['Number of Reviews', 'Maximum Number of Reviews', 'Review Scores'])
        with tab1:
            number_of_reviews = feature.feature('number_of_reviews')
            plotly.vertical_bar_chart(df=number_of_reviews, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Number of Reviews', title_x=0.43)
        with tab2:
            max_number_of_reviews = feature.feature(
                'number_of_reviews', order='number_of_reviews desc')
            plotly.vertical_bar_chart(df=max_number_of_reviews, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Maximum Number of Reviews', title_x=0.35)
        with tab3:
            review_scores = feature.feature('review_scores')
            plotly.vertical_bar_chart(df=review_scores, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Review Scores', title_x=0.43)


class host:

    def countries_list():
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select distinct country
                           from airbnb
                           order by country asc;""")
        gopi.commit()
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=['Country'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        return data

    def column_value(country, column_name, limit=10):
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select {column_name}, count({column_name}) as count
                           from airbnb
                           where country='{country}'
                           group by {column_name}
                           order by count desc
                           limit {limit};""")
        gopi.commit()
        s = cursor.fetchall()
        data = pd.DataFrame(s, columns=[column_name, 'count'])
        return data[column_name].values.tolist()

    def column_value_names(country, column_name, order='desc', limit=10):
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select {column_name}, count({column_name}) as count
                           from airbnb
                           where country='{country}'
                           group by {column_name}
                           order by {column_name} {order}
                           limit {limit};""")
        gopi.commit()
        s = cursor.fetchall()
        data = pd.DataFrame(s, columns=[column_name, 'count'])
        return data[column_name].values.tolist()

    def column_value_count_not_specified(country, column_name, limit=10):
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select {column_name}, count({column_name}) as count
                           from airbnb
                           where country='{country}' and {column_name}!='Not Specified'
                           group by {column_name}
                           order by count desc
                           limit {limit};""")
        gopi.commit()
        s = cursor.fetchall()
        data = pd.DataFrame(s, columns=[column_name, 'count'])
        return data[column_name].values.tolist()

    def host(country, column_name, column_value, limit=10):
        gopi = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='root',
                                database='airbnb')
        cursor = gopi.cursor()
        cursor.execute(f"""select distinct host_id, count(host_id) as count
                           from airbnb
                           where country='{country}' and {column_name}='{column_value}'
                           group by host_id
                           order by count desc
                           limit {limit};""")
        gopi.commit()
        s = cursor.fetchall()
        i = [i for i in range(1, len(s)+1)]
        data = pd.DataFrame(s, columns=['host_id', 'count'], index=i)
        data = data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        data['percentage'] = data['count'].apply(
            lambda x: str('{:.2f}'.format(x/55.55)) + '%')
        data['y'] = data['host_id'].apply(lambda x: str(x)+'`')
        return data

    def main(values, label):
        col1, col2, col3 = st.columns(3)
        with col1:
            a = str(values) + '_column_value_list'
            b = str(values) + '_column_value'

            a = host.column_value(country=country, column_name=values)
            b = st.selectbox(label=label, options=a)

            values = host.host(country=country, column_name=values,
                               column_value=b)
            return values

    def main_min(values, label):
        col1, col2, col3 = st.columns(3)
        with col1:
            a = str(values) + '_column_value_list'
            b = str(values) + '_column_value'

            a = host.column_value_names(
                country=country, column_name=values, order='asc')
            b = st.selectbox(label=label, options=a)

            values = host.host(country=country, column_name=values,
                               column_value=b)
            return values

    def main_max(values, label):
        col1, col2, col3 = st.columns(3)
        with col1:
            a = str(values) + '_column_value_list'
            b = str(values) + '_column_value'

            a = host.column_value_names(
                country=country, column_name=values, order='desc')
            b = st.selectbox(label=label, options=a)

            values = host.host(country=country, column_name=values,
                               column_value=b)
            return values

    def not_specified(values, label):
        col1, col2, col3 = st.columns(3)
        with col1:
            a = str(values) + '_column_value_list'
            b = str(values) + '_column_value'

            a = host.column_value_count_not_specified(
                country=country, column_name=values)
            b = st.selectbox(label=label, options=a)

            values = host.host(country=country, column_name=values,
                               column_value=b)
            return values

    def host_analysis():

        # vertical_bar chart
        property_type = host.main(
            values='property_type', label='Property Type')
        plotly.vertical_bar_chart(df=property_type, x='y', y='count', text='percentage',
                                  color='#5D9A96', title='Property Type', title_x=0.45)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Room Type', 'Bed Type'])
        with tab1:
            room_type = host.main(values='room_type', label='')
            plotly.vertical_bar_chart(df=room_type, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Room Type', title_x=0.45)
        with tab2:
            bed_type = host.main(values='bed_type', label='')
            plotly.vertical_bar_chart(df=bed_type, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bed Type', title_x=0.45)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Minimum Nights', 'Maximum Nights'])
        with tab1:
            minimum_nights = host.main(values='minimum_nights', label='')
            plotly.vertical_bar_chart(df=minimum_nights, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Minimum Nights', title_x=0.45)
        with tab2:
            maximum_nights = host.main(values='maximum_nights', label='')
            plotly.vertical_bar_chart(df=maximum_nights, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Maximum Nights', title_x=0.45)

        # vertical_bar chart
        cancellation_policy = host.main(
            values='cancellation_policy', label='Cancellation Policy')
        plotly.vertical_bar_chart(df=cancellation_policy, x='y', y='count', text='percentage',
                                  color='#5cb85c', title='Cancellation Policy', title_x=0.45)

        # vertical_bar chart
        tab1, tab2 = st.tabs(
            ['Minimum Accommodates', 'Maximum Accommodates'])
        with tab1:
            minimum_accommodates = host.main_min(
                values='accommodates', label='')
            plotly.vertical_bar_chart(df=minimum_accommodates, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Minimum Accommodates', title_x=0.45)
        with tab2:
            maximum_accommodates = host.main_max(
                values='accommodates', label='')
            plotly.vertical_bar_chart(df=maximum_accommodates, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Maximum Accommodates', title_x=0.45)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(
            ['Bedrooms', 'Minimum Beds', 'Maximum Beds', 'Bathrooms'])
        with tab1:
            bedrooms = host.main(values='bedrooms', label='')
            plotly.vertical_bar_chart(df=bedrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bedrooms', title_x=0.45)
        with tab2:
            minimum_beds = host.main_min(values='beds', label='')
            plotly.vertical_bar_chart(df=minimum_beds, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Minimum Beds', title_x=0.45)
        with tab3:
            maximum_beds = host.main_max(values='beds', label='')
            plotly.vertical_bar_chart(df=maximum_beds, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Maximum Beds', title_x=0.45)
        with tab4:
            bathrooms = host.main(values='bathrooms', label='')
            plotly.vertical_bar_chart(df=bathrooms, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Bathrooms', title_x=0.45)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(
            ['Price', 'Minimum Price', 'Maximum Price', 'Cleaning Fee'])
        with tab1:
            price = host.main(values='price', label='')
            plotly.vertical_bar_chart(df=price, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Price', title_x=0.45)
        with tab2:
            minimum_price = host.main_min(values='price', label='')
            plotly.vertical_bar_chart(df=minimum_price, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Minimum Price', title_x=0.45)
        with tab3:
            maximum_price = host.main_max(values='price', label='')
            plotly.vertical_bar_chart(df=maximum_price, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Maximum price', title_x=0.45)
        with tab4:
            cleaning_fee = host.not_specified(
                values='cleaning_fee', label='')
            plotly.vertical_bar_chart(df=cleaning_fee, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Cleaning Fee', title_x=0.45)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(['Guests Included', 'Cost at Extra People',
                                          'Minimum Cost at Extra People', 'Maximum Cost at Extra People'])
        with tab1:
            guests_included = host.main(values='guests_included', label='')
            plotly.vertical_bar_chart(df=guests_included, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Guests Included', title_x=0.45)
        with tab2:
            extra_people = host.main(values='extra_people', label='')
            plotly.vertical_bar_chart(df=extra_people, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Cost at Extra People', title_x=0.45)
        with tab3:
            extra_people_min_cost = host.main_min(
                values='extra_people', label='')
            plotly.vertical_bar_chart(df=extra_people_min_cost, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Minimum Cost at Extra People', title_x=0.45)
        with tab4:
            extra_people_max_cost = host.main_max(
                values='extra_people', label='')
            plotly.vertical_bar_chart(df=extra_people_max_cost, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Maximum Cost at Extra People', title_x=0.45)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Response Time', 'Response Rate'])
        with tab1:
            host_response_time = host.main(
                values='host_response_time', label='')
            plotly.vertical_bar_chart(df=host_response_time, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Response Time', title_x=0.45)
        with tab2:
            host_response_rate = host.not_specified(
                values='host_response_rate', label='')
            plotly.vertical_bar_chart(df=host_response_rate, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Response Rate', title_x=0.45)

        # vertical_bar chart
        tab1, tab2, tab3, tab4 = st.tabs(
            ['Availability 30', 'Availability 60', 'Availability 90', 'Availability 365'])
        with tab1:
            availability_30 = host.main_max(
                values='availability_30', label='')
            plotly.vertical_bar_chart(df=availability_30, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability of Next 30 Days', title_x=0.45)
        with tab2:
            availability_60 = host.main_max(
                values='availability_60', label='')
            plotly.vertical_bar_chart(df=availability_60, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability of Next 60 Days', title_x=0.45)
        with tab3:
            availability_90 = host.main_max(
                values='availability_90', label='')
            plotly.vertical_bar_chart(df=availability_90, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability of Next 90 Days', title_x=0.45)
        with tab4:
            availability_365 = host.main_max(
                values='availability_365', label='')
            plotly.vertical_bar_chart(df=availability_365, x='y', y='count', text='percentage',
                                      color='#5cb85c', title='Availability of Next 365 Days', title_x=0.45)

        # vertical_bar chart
        tab1, tab2 = st.tabs(['Number of Reviews', 'Review Scores'])
        with tab1:
            number_of_reviews = host.main_max(
                values='number_of_reviews', label='')
            plotly.vertical_bar_chart(df=number_of_reviews, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Number of Reviews', title_x=0.45)
        with tab2:
            review_scores = host.main_max(values='review_scores', label='')
            plotly.vertical_bar_chart(df=review_scores, x='y', y='count', text='percentage',
                                      color='#5D9A96', title='Review Scores', title_x=0.45)


# streamlit title, background color and tab configuration
streamlit_config()
st.write('')


with st.sidebar:
    image_url = 'https://raw.githubusercontent.com/gopiashokan/Airbnb-Analysis/main/airbnb_banner.jpg'
    st.image(image_url, use_column_width=True)

    option = option_menu(menu_title='', options=['Migrating to SQL', 'Features Analysis', 'Host Analysis', 'Exit'],
                         icons=['database-fill', 'list-task', 'person-circle', 'sign-turn-right-fill'])
    col1, col2, col3 = st.columns([0.26, 0.48, 0.26])
    with col2:
        button = st.button(label='Submit')


if button and option == 'Migrating to SQL':
    st.write('')
    sql.create_table()
    sql.delete_table()
    sql.data_migration()
    st.success('Successfully Data Migrated to SQL Database')
    st.balloons()


elif option == 'Features Analysis':
    try:
        st.write('')
        feature.feature_analysis()

    except:
        col1, col2 = st.columns(2)
        with col1:
            st.info('SQL Database is Currently Empty')


elif option == 'Host Analysis':
    try:
        st.write('')
        col1, col2, col3 = st.columns(3)
        with col1:
            countries_list = host.countries_list()
            country = st.selectbox(label='Country', options=countries_list)
        if country:
            host.host_analysis()

    except:
        col1, col2 = st.columns(2)
        with col1:
            st.info('SQL Database is Currently Empty')


elif option == 'Exit':
    st.write('')
    gopi = psycopg2.connect(host='localhost',
                            user='postgres',
                            password='root',
                            database='airbnb')
    cursor = gopi.cursor()
    gopi.close()

    st.success('Thank you for your time. Exiting the application')
    st.balloons()

