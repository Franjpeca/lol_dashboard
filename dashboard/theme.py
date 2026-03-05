"""
dashboard/theme.py
Paleta de colores, estilos y helpers de layout compartidos.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import math

# ─── Colors ──────────────────────────────────────────────────────────────────

GOLD   = "#FCD34D"
BLUE   = "#60A5FA"
GREEN  = "#34D399"
RED    = "#F87171"
BG     = "#0A0A0B"
PAPER  = "#121214"
BORDER = "#27272A"
TEXT   = "#F4F4F5"
MUTED  = "#A1A1AA"

# Paleta por defecto
CHART_SCALE = "Turbo"


# ─── Chart helpers ────────────────────────────────────────────────────────────

def make_hbar(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color_scale: str = "Turbo",
    text_fmt: str = None,
    xrange=None,
    height: int = None,
    color_col: str = None,
    hover_col: str = None,
    color_range: list = None,
    color_mid: float = None,        # Punto neutro (ej. 50 para winrate)
    show_colorbar: bool = False,    # Mostrar leyenda de color
    colorbar_title: str = None,     # Título de la leyenda
    color_transform: str = None,    # None | "log1p" | "sqrt"
) -> go.Figure:
    """
    Genera un gráfico de barras horizontales usando go.Bar con soporte para:
    - Escalas divergentes (color_mid)
    - Transformaciones estadísticas (log, sqrt)
    - Colorbars configurables
    """
    n = max(1, len(df))
    tick_font = max(11, min(20, int(280 / n)))
    auto_height = int(max(380, min(3000, 38 * n)))
    h = height or auto_height

    # 1. Determinar columna y valores de color
    c_col = color_col or x
    c_raw = pd.to_numeric(df[c_col], errors='coerce').fillna(0)
    
    # 2. Aplicar transformación
    if color_transform == "log1p":
        c_values = np.log1p(c_raw).tolist()
    elif color_transform == "sqrt":
        c_values = np.sqrt(c_raw).tolist()
    elif color_transform == "rank":
        # Rank normalizado [0, 1] → máxima utilización del gradiente, contraste óptimo
        ranked = c_raw.rank(method="average")
        c_values = ((ranked - ranked.min()) / (ranked.max() - ranked.min() + 1e-9)).tolist()
    else:
        c_values = c_raw.tolist()

    # 3. Preparar templates
    text_template = f"%{{x{text_fmt}}}" if text_fmt else "%{x}"
    cust_data = df[hover_col].tolist() if hover_col else None
    
    hover_tpl = (
        f"%{{y}}<br>{x}: %{{x{text_fmt}}}<br>{hover_col}: %{{customdata:.2f}}<extra></extra>"
        if hover_col else
        f"%{{y}}<br>{x}: %{{x{text_fmt if text_fmt else ''}}}<extra></extra>"
    )

    # 4. Crear traza
    bar = go.Bar(
        x=df[x].tolist(),
        y=df[y].tolist(),
        orientation="h",
        text=df[x].tolist(),
        texttemplate=text_template,
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=max(11, min(16, int(200 / n)))),
        marker=dict(
            color=c_values,
            colorscale=color_scale,
            showscale=show_colorbar,
            cmid=color_mid,
            cmin=color_range[0] if color_range else None,
            cmax=color_range[1] if color_range else None,
            colorbar=dict(
                title=dict(text=colorbar_title, font=dict(size=10, color=MUTED)),
                thickness=15,
                len=0.7,
                tickfont=dict(size=9, color=MUTED)
            ) if show_colorbar else None,
            line=dict(width=0),
        ),
        customdata=cust_data,
        hovertemplate=hover_tpl,
        showlegend=False,
    )

    fig = go.Figure(data=[bar])
    fig.update_layout(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        showlegend=False,
        font=dict(color=TEXT, family="Inter, sans-serif"),
        title=dict(
            text=title,
            font=dict(color=GOLD, size=14),
            pad=dict(b=10),
        ),
        height=h,
        bargap=0.18,
        margin=dict(l=160, r=80 if show_colorbar else 60, t=60, b=40),
        xaxis=dict(
            showgrid=True,
            gridcolor=BORDER,
            tickfont=dict(size=11),
            color=TEXT,
            title="",
            range=xrange,
        ),
        yaxis=dict(
            type="category",
            categoryorder="array",
            categoryarray=df[y].tolist(),
            tickfont=dict(size=tick_font),
            automargin=True,
            showgrid=False,
            color=TEXT,
        ),
        hoverlabel=dict(bgcolor="#111", font_color=TEXT, bordercolor=BORDER),
    )
    return fig

GLOBAL_CSS = f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=Outfit:wght@700;800&display=swap');

    .stApp {{
        background-color: {BG};
    }}

    .lol-brand-gradient {{
        background: linear-gradient(90deg, #FCD34D, #60A5FA);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-family: 'Outfit', sans-serif;
        font-weight: 800;
        font-size: 1.8rem;
        padding-top: 5px;
    }}

    .lol-badge {{
        background: rgba(52, 211, 153, 0.1);
        color: #34D399;
        padding: 4px 12px;
        border-radius: 99px;
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        border: 1px solid rgba(52, 211, 153, 0.2);
    }}

    /* Ocultar header, menu y footer por defecto de Streamlit */
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    footer {{visibility: hidden;}}

    /* Eliminar el espacio en blanco de arriba dejado por el header */
    .block-container {{
        padding-top: 1rem;
        padding-bottom: 5rem;
    }}

    /* Estilo de Tabs de Streamlit */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 24px;
        background-color: transparent;
        display: flex;
        justify-content: center;
    }}

    .stTabs [data-baseweb="tab"] {{
        height: auto;
        white-space: pre;
        background-color: transparent;
        border-radius: 4px 4px 0 0;
        gap: 1px;
        padding-top: 16px;
        padding-bottom: 16px;
        border-bottom: 3px solid transparent;
    }}

    .stTabs [data-baseweb="tab"] p {{
        font-size: 1.3rem;
        font-weight: 700;
        color: {MUTED};
    }}

    .stTabs [aria-selected="true"] {{
        border-bottom: 3px solid {GOLD} !important;
    }}

    .stTabs [aria-selected="true"] p {{
        color: {GOLD} !important;
    }}

    /* Mejora de scrollbars */
    ::-webkit-scrollbar {{
        width: 8px;
        height: 8px;
    }}
    ::-webkit-scrollbar-track {{
        background: {BG};
    }}
    ::-webkit-scrollbar-thumb {{
        background: {BORDER};
        border-radius: 4px;
    }}
    ::-webkit-scrollbar-thumb:hover {{
        background: {MUTED};
    }}

    /* Aumentar tamaño de selectbox (menús de selección del header) */
    div[data-baseweb="select"] {{
        font-size: 1.15rem;
    }}
    div[data-baseweb="popover"] {{
        font-size: 1.15rem;
    }}
</style>
"""
