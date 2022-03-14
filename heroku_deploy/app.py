import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go

st.title("Data Incubator Captsone Project")

st.write("""
	### This app will find potential solar location based on the latitude and longitude in New York City and Albany. 

	""")

st.sidebar.header("User Input Parameter for first plot")

select_option = st.sidebar.selectbox("Select option you want to plot", ("Select", "power_MWH", "ghi", "total_area_m2"))

df_pandas = pd.read_parquet("final_data")
if select_option in ["power_MWH", "ghi", "total_area_m2"]:

	fig = px.scatter(df_pandas, x="longitude", y="latitude", color= select_option,
	                 size= select_option, color_continuous_scale = 'rainbow',hover_data=['city'])
	st.plotly_chart(fig, use_container_width=True)
else:
	fig = px.scatter(df_pandas, x="longitude", y="latitude", color= "power_MWH",
	                 size= "power_MWH", color_continuous_scale = 'rainbow',hover_data=['city'])
	st.plotly_chart(fig, use_container_width=True)

st.write("""
	### Plot is based on based on the solar rooftop area. 
	### Color is based on rootop area and size of dot is based on solar power.
	""")

fig1 = px.scatter(df_pandas, x="longitude", y="latitude", color="total_area_m2",
                 size='power_MWH', color_continuous_scale = 'rainbow',hover_data=['city'])
st.plotly_chart(fig1, use_container_width=True)


st.write("""
	### Solar Power is plotted for New York City and Albany.
	""")
df_pandas['text'] = df_pandas['city'] +  ':' + df_pandas['power_MWH'].astype(str)
fig2 = go.Figure(data=go.Scattergeo(
        locationmode = 'USA-states',
        lon = df_pandas['longitude'],
        lat = df_pandas['latitude'],
        text = df_pandas['text'],
        mode = 'markers',
        marker = dict(
            size = 8,
            opacity = 0.8,
            reversescale = True,
            autocolorscale = False,
            symbol = 'square',
            line = dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
            colorscale = 'rainbow',
            cmin = 0,
            color = df_pandas['power_MWH'],
            cmax = df_pandas['power_MWH'].max(),
            colorbar_title="Solar Power"
        )))

fig2.update_layout(
        title = 'Solar Power (NYC and Albany)',
        geo = dict(
            scope='usa',
            projection_type='albers usa',
            showland = True,
            landcolor = "rgb(250, 250, 250)",
            subunitcolor = "rgb(217, 217, 217)",
            countrycolor = "rgb(217, 217, 217)",
            countrywidth = 0.5,
            subunitwidth = 0.5
        ),
    )
st.plotly_chart(fig2, use_container_width=True)
