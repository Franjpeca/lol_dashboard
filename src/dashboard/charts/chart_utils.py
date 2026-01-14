"""
Utility functions for chart error handling and empty state rendering.
"""
import plotly.graph_objs as go
from dash import html


def create_empty_message(chart_name: str, reason: str = "contiene errores o datos vacíos") -> html.Div:
    """
    Creates a simple text message instead of an empty chart.
    Returns a Dash HTML Component (Div).
    """
    return html.Div([
        html.P(
            f"La gráfica {chart_name} {reason}",
            style={
                "textAlign": "center",
                "padding": "40px",
                "color": "#666",
                "fontSize": "0.95em",
                "fontStyle": "italic"
            }
        )
    ], style={
        "backgroundColor": "#1a1a1a",
        "borderRadius": "8px",
        "border": "1px solid #333",
        "margin": "10px 0"
    })


def create_empty_figure(title: str = "No Data", message: str = "No data available") -> go.Figure:
    """
    Creates a Plotly Figure suitable for dcc.Graph 'figure' property
    that displays a message in a dark theme.
    """
    fig = go.Figure()
    
    # Add text annotation
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=14, color="#666")
    )
    
    # Configure dark layout
    fig.update_layout(
        title=dict(text=title, font=dict(color="#ccc")),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="#1a1a1a",
        paper_bgcolor="#1a1a1a",  # Match dashboard card color
        margin=dict(l=20, r=20, t=40, b=20),
        height=300
    )
    
    return fig


def safe_render(render_func, *args, **kwargs):
    """
    Safely executes a render function and returns simple message on error.
    Returns a list of Components (for layout injection).
    """
    try:
        result = render_func(*args, **kwargs)
        
        # If result is None or empty list, return message
        if not result:
            chart_name = render_func.__module__.split('.')[-1].replace('chart_', '').replace('_', ' ').title()
            return [create_empty_message(chart_name, "no tiene datos disponibles")]
        
        return result
        
    except FileNotFoundError as e:
        print(f"[CHART ERROR] File not found: {e}")
        chart_name = render_func.__module__.split('.')[-1].replace('chart_', '').replace('_', ' ').title()
        return [create_empty_message(chart_name, "no tiene datos generados aún")]
        
    except KeyError as e:
        print(f"[CHART ERROR] Missing data key: {e}")
        chart_name = render_func.__module__.split('.')[-1].replace('chart_', '').replace('_', ' ').title()
        return [create_empty_message(chart_name, "contiene errores en los datos")]
        
    except Exception as e:
        print(f"[CHART ERROR] Unexpected error: {type(e).__name__}: {e}")
        chart_name = render_func.__module__.split('.')[-1].replace('chart_', '').replace('_', ' ').title()
        return [create_empty_message(chart_name, "contiene errores")]


def create_error_message(title: str, error_type: str = "No Data") -> html.Div:
    """
    Creates an HTML error message component.
    """
    message_map = {
        "No Data": "no tiene datos disponibles",
        "Error": "contiene errores",
        "Missing File": "no tiene datos generados aún"
    }
    
    return create_empty_message(title, message_map.get(error_type, "contiene errores"))
