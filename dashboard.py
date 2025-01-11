from dash import Dash, dcc, html, Output, Input
import dash_cytoscape as cyto
import plotly.express as px

from delta import configure_spark_with_delta_pip

import networkx as nx
import matplotlib.pyplot as plt

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from core.delta_lake_manager import DeltaLakeManager
from core.environment import Environment

from metadata import deltalake
from metadata.columns import StackExchangeColumns as columns

import os
import pandas as pd


def tag_frequency_through_time_figure(df):
    chart = px.line(
        df,
        x="PartitionDate",
        y="Count",
        color="Tag",
        title="Top 10 tags usage over time",
        markers=True
    )
    chart.update_layout(
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        xaxis_title='Date',
        yaxis_title='Number tag usages in questions',
        title_font=dict(family='Arial', color='black')
    )
    return chart


def activity_freq_diagram(df):
    chart = px.bar(
        df,
        x="Category",
        y="Count",
        title="Number of publications",
        color="Category"
    )
    chart.update_layout(
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        showlegend=False,
        xaxis_title='Publication',
        yaxis_title='Count',
        title_font=dict(family='Arial', color='black')
    )
    return chart


def find_and_create_figure(environment, folder, callback):
    full_path = f"{environment.data_directory_path()}{folder}"
    csv_files = [file for file in os.listdir(full_path) if file.endswith('.csv')]
    assert len(csv_files) > 0
    csv_file_path = os.path.join(full_path, csv_files[0])
    df = pd.read_csv(csv_file_path)
    fig = callback(df)
    return fig


def find_and_create_figure_with_parameter(environment, folder, callback, parameter):
    full_path = f"{environment.data_directory_path()}{folder}"
    csv_files = [file for file in os.listdir(full_path) if file.endswith('.csv')]
    assert len(csv_files) > 0
    csv_file_path = os.path.join(full_path, csv_files[0])
    df = pd.read_csv(csv_file_path)
    fig = callback(df, parameter)
    return fig


def top_most_frequent_tags(df):
    barchart = px.bar(df, x="items", y="freq", title="Top used tags", color_discrete_sequence=["#ff6f00"])
    barchart.update_layout(
        plot_bgcolor='#ffffff',
        paper_bgcolor='#ffffff',
        title_font=dict(family='Arial', color='black'),
        xaxis_title='Tag',
        yaxis_title='Number of usages',
    )
    return barchart


def association_rules(df):
    G = nx.DiGraph()

    for index, row in df.iterrows():
        antecedent = row['antecedent']
        consequent = row['consequent']
        # confidence = row['confidence']
        support = row['support']
    
        G.add_edge(antecedent, consequent, weight=support, label=f"{support:.4f}")

    pos = nx.spring_layout(
        G, 
        k=0.8,
        seed=24
    )
    def coordinate(value, max_value):
        new_value = int((float(value) + 1) / 2 * max_value)
        return new_value
    nodes = [{'data': {'id': str(node), 'label': str(node)}, 'position': {'x': coordinate(pos[node][0], 250), 'y': coordinate(pos[node][1], 180)}} for node in G.nodes]
    edges = [{'data': {'source': str(u), 'target': str(v), 'content': d['label']}} for u, v, d in G.edges(data=True)]

    return cyto.Cytoscape(
            id='assosiation_graph',
            style={'width': '90%', 'height': '300px'},
            layout={'name': 'preset'},
            # layout={'name': 'circle'},
            elements=(nodes + edges),
            stylesheet=[
                {
                    'selector': 'node',
                    'style': {
                        'background-color': '#ff6f00',
                        'label': 'data(label)',
                        'color': '#000000',
                        'font-size': '9px'
                    }
                },
                {
                    'selector': 'edge',
                    'style': {
                        'width': 1,
                        'line-color': '#ccc',
                        'label': 'data(content)',
                        'font-size': '8px'
                    }
                }
            ]
        )


def top_by_column_callback(column_name, smallest, title, y_axis, limit=100000):
    def top_by_column(df):
        if smallest:
            top5 = df.head(limit).nsmallest(10, column_name)
        else:
            top5 = df.head(limit).nlargest(10, column_name)
        fig = px.bar(
            top5, 
            y='Tag', 
            x=column_name, 
            title=title,
            color='Tag',
            orientation='h',
        )
        # fig.update_traces(
        #     text=df['Tag'],
        #     textposition='inside',
        #     textfont=dict(color='white')
        # )
        # fig.update_yaxes(
        #     # tickmode="array",
        #     # categoryorder="total ascending",
        #     # tickvals=x,
        #     # ticktext=x,
        #     ticklabelposition="inside",
        #     # tickfont=dict(color="white"),
        # )
        fig.update_layout(
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            showlegend=False,
            xaxis_title=y_axis,
            title_font=dict(family='Arial', color='black'),
            xaxis=dict(
                showline=True,
                linecolor='black',
                linewidth=1
            ),
            yaxis=dict(
                showline=True,
                linecolor='black',
                linewidth=1
            )
        )
        return fig
    return top_by_column


def leaderboard(
    title,
    columns_name,
    y_axis_title,
    environment
):
    return html.Div([
                html.H2(title, style={'font-family': 'Arial', 'text-align': 'center'}),
                html.H3("(among top 50 most questioned)", style={'font-family': 'Arial', 'text-align': 'center'}),
                html.Div([
                    dcc.Graph(
                        figure=find_and_create_figure(environment, "/final_df", top_by_column_callback(columns_name, False, 'Top 10', y_axis_title, 50)),
                        config={'displayModeBar': False},
                        style={'width': '50%', 'height': '480px', 'display': 'inline-block'}
                    ),
                    dcc.Graph(
                        figure=find_and_create_figure(environment, "/final_df", top_by_column_callback(columns_name, True, 'Bottom 10', y_axis_title, 50)),
                        config={'displayModeBar': False},
                        style={'width': '50%', 'height': '480px', 'display': 'inline-block'}
                    ),
                ], style={'display': 'flex', 'margin-left': '10px'})
            ], style={'border': '1px solid #ff6f00', 'margin-bottom': '10px'})


def interactive_section():

    folder = '/final_df'
    environment = Environment()
    full_path = f"{environment.data_directory_path()}{folder}"
    csv_files = [file for file in os.listdir(full_path) if file.endswith('.csv')]
    assert len(csv_files) > 0
    csv_file_path = os.path.join(full_path, csv_files[0])
    df = pd.read_csv(csv_file_path)


    return html.Div([
        html.H2("Tag overview", style={'font-family': 'Arial', 'text-align': 'center'}),
        dcc.Dropdown(
            id='dropdown-1',
            options=[
                {'label': tag, 'value': tag} for tag in df.head(50)['Tag'].unique()
            ],
            placeholder="Choose an option",
            multi=False,
            style={
                # 'width': '90%',
                # 'margin': '10px',
                # 'align-items': 'center',
                'margin-bottom': '10px'
            }
        ),
        html.Div(id='dropdown-figure')
    ], style={'border': '1px solid #ff6f00', 'margin-bottom': '10px', 'align-items': 'center'})


def tag_piechart(df, tag):
    filtered_df = df[df['Tag'] == tag]
    a = filtered_df['SumCommentsCount'].iloc[0]
    p = filtered_df['SumPositiveComments'].iloc[0]
    values = [a - p, p]
    labels = ['negative', 'positive']
    fig = px.pie(values=values, names=labels, title=f"Comments")
    return fig


def tag_piechart_2(df, tag):
    filtered_df = df[df['Tag'] == tag]
    q = filtered_df['QuestionsCount'].iloc[0]
    a = filtered_df['SumAcceptedAnswerCount'].iloc[0]
    values = [q - a, a]
    labels = ['not_answered', 'answered']
    fig = px.pie(values=values, names=labels, title=f"Questions")
    return fig


def chart_0(df, tag):
    filtered_df = df[df['Tag'] == tag]
    q = filtered_df['QuestionsCount'].iloc[0]
    a = filtered_df['SumAnswersCount'].iloc[0]
    c = filtered_df['SumCommentsCount'].iloc[0]
    pd_df = pd.DataFrame({
        'Category': ["Questions", "Answers", "Comments"],
        'Count': [q, a, c],
    })
    return activity_freq_diagram(pd_df)

def tag_dashboard(df, tag):
    return html.Div([
                # html.H2("General Overview", style={'font-family': 'Arial', 'text-align': 'center'}),
                html.Div([
                    dcc.Graph(
                        figure=chart_0(df, tag),
                        config={'displayModeBar': False},
                        style={'width': '33%', 'height': '400px', 'display': 'inline-block'}
                    ),
                    dcc.Graph(
                        figure=tag_piechart(df, tag),
                        config={'displayModeBar': False},
                        style={'width': '33%', 'height': '400px', 'display': 'inline-block'}
                    ),
                    dcc.Graph(
                        figure=tag_piechart_2(df, tag),
                        config={'displayModeBar': False},
                        style={'width': '33%', 'height': '400px', 'display': 'inline-block'}
                    ),
                ], style={'display': 'flex'}),
            ], style={'height': '400px'})


def main():
    environment = Environment()

    directory_fig = {
        "/tags_frequency": tag_frequency_through_time_figure
    }

    app = Dash(__name__)

    app.layout = html.Div([
        html.H1("Stack Overflow Tags Popularity Analysis", style={'font-family': 'Arial', 'text-align': 'center'}),
        html.Div([
            html.H2("General Overview", style={'font-family': 'Arial', 'text-align': 'center'}),
            html.Div([
                dcc.Graph(
                    figure=find_and_create_figure(environment, "/general_info", activity_freq_diagram),
                    config={'displayModeBar': False},
                    style={'width': '33%', 'height': '400px', 'display': 'inline-block'}
                ),
                dcc.Graph(
                    figure=find_and_create_figure(environment, "/tags_frequency", tag_frequency_through_time_figure),
                    config={'displayModeBar': False},
                    style={'width': '67%', 'height': '400px', 'display': 'inline-block'}
                ),
            ], style={'display': 'flex'}),
        ], style={'border': '1px solid #ff6f00', 'margin-bottom': '10px'}),
        html.Div([
            html.H2("Frequent patterns", style={'font-family': 'Arial', 'text-align': 'center'}),
            html.Div([
                dcc.Graph(
                    figure=find_and_create_figure(environment, "/top_most_frequent_tags", top_most_frequent_tags),
                    config={'displayModeBar': False},
                    style={'width': '60%', 'height': '400px', 'display': 'inline-block'}
                ),
                html.Div([
                    html.H4("Common assosiation rules (with support value)", style={'font-family': 'Arial', 'font-weight': 'normal'}),
                    find_and_create_figure(environment, "/association_rules_df", association_rules),
                ], style={'width': '40%', 'margin-bottom': '30px', 'margin-right': '30px'}),
                # html.H3("Tags assosiation rules based on support")
            ], style={'display': 'flex'})
        ], style={'border': '1px solid #ff6f00', 'margin-bottom': '10px'}),

        leaderboard(
            "Leaderboards by number of answers",
            "AnswersPerQuestions",
            'Average number of answers per question',
            environment
        ),

        leaderboard(
            "Leaderboards by number of accepted answers",
            "AcceptedAnswersPerQuestions",
            'Average number of accepted answers per question',
            environment
        ),

        leaderboard(
            "Leaderboards by owner-answered questions",
            "SelfAnsweredPerQuestions",
            'Portion of owner-answered questions',
            environment
        ),

        leaderboard(
            "Leaderboards by average answer waiting time",
            "AvgAcceptedAnswerAppearenceTimeInHours",
            'Average time (hours)',
            environment
        ),

        leaderboard(
            "Leaderboards by questions views count",
            "AvgQuestionViewCount",
            'Average views count',
            environment
        ),

        leaderboard(
            "Leaderboards by questions length",
            "AvgQuestionWordsCount",
            'Average number of words',
            environment
        ),

        leaderboard(
            "Leaderboards by accepted answers length",
            "AvgAcceptedAnswerWordsCount",
            'Average number of words',
            environment
        ),

        leaderboard(
            "Leaderboards by comments per question",
            "CommentsCountPerQuestion",
            'Average number of comments per question',
            environment
        ),

        leaderboard(
            "Leaderboards by positive comments per question",
            "PositiveCommentsPerQuestion",
            'Average number of positive comments per question',
            environment
        ),

        interactive_section()
    ])

    @app.callback(
        Output('dropdown-figure', 'children'),
        Input('dropdown-1', 'value')
    )
    def update_interactive_section(tag):
        if tag is None:
            return html.Div([], style={'height': '400px'})
        return find_and_create_figure_with_parameter(environment, "/final_df", tag_dashboard, tag)

    app.run_server(debug=True)


if __name__ == "__main__":
    main()
