from pyjstat import pyjstat
import requests
import pandas as pd
import numpy as np
import plotly.graph_objs as go


ICT_URL = "http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/tin00074?nace_r2=ICT"
CCS_URL = "http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/isoc_cicce_use?sizen_r2=M_C10_S951_XK&sizen_r2=L_C10_S951_XK&unit=PC_ENT&indic_is=E_CC"

pd.set_option('max_columns', None)


def extract(url: str) -> pd.DataFrame:
    try:
        dataset = pyjstat.Dataset.read(url)
        es_df = dataset.write('dataframe')
    except requests.exceptions.RequestException as e:
        raise Exception(f"Can't get data from {url}!\n", e)
    es_df["Country"] = es_df["geo"]
    es_df = es_df[["Country", "time", "value"]]
    return es_df.fillna("1").convert_dtypes()


def transform(ict_df: pd.DataFrame, ccs_df: pd.DataFrame, csv_gdp_df: pd.DataFrame) -> pd.DataFrame:
    ict_css_join_df = pd.merge(ccs_df, ict_df, how='left', on=['Country', 'Year'])
    final_df = pd.merge(ict_css_join_df, csv_gdp_df, how='left', on=['Country', 'Year']).fillna(1)
    final_df["CCS_value"] = pd.to_numeric(final_df["CCS_value"])
    final_df["ICT_value"] = pd.to_numeric(final_df["ICT_value"])
    final_df["AOC"] = final_df["GDP_value"]*final_df["CCS_value"]*final_df["ICT_value"]
    # final_df_group = final_df.groupby(['Country', 'Year'])['AOC'].agg('sum')
    # print(final_df_group)
    return final_df


def visualize(aoc_df: pd.DataFrame):
    df_vis = pd.pivot_table(aoc_df, values='AOC', index=['Year'], columns='Country', aggfunc=np.sum)
    fig = go.Figure()
    for col in df_vis.columns:
        fig.add_trace(go.Scatter(x=df_vis.index, y=df_vis[col].values,
                                 name=col,
                                 mode='markers+lines',
                                 line=dict(shape='linear'),
                                 connectgaps=True
                                 )
                      )
    fig.show()


if __name__ == '__main__':
    ict_df = extract(ICT_URL).rename(columns={'time': 'Year', 'value': 'ICT_value'})
    ccs_df = extract(CCS_URL).rename(columns={'time': 'Year', 'value': 'CCS_value'})

    gdp_df = pd.read_csv("gdp_data.csv", delimiter='|').fillna("1").convert_dtypes()
    year_columns_list = gdp_df.columns.drop("Country")
    gdp_df = gdp_df[gdp_df.apply(lambda x: not any(x.str.contains('Office')), axis=1)]
    gdp_df = pd.melt(gdp_df, id_vars=['Country'], value_vars=year_columns_list, var_name='Year', value_name='GDP_value')
    gdp_df["GDP_value"] = gdp_df['GDP_value'].str.replace(',', '.').astype(float)

    df = transform(ict_df, ccs_df, gdp_df)
    visualize(df)