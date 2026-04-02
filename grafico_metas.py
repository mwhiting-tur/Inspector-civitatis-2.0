import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import matplotlib.patheffects as path_effects

# 1. Data de Argentina Q1 + Meta Abril
data = {
    'Destino': ['San Pedro de Atacama', 'Puerto Natales', 'Santiago', 'Rapa Nui', 'Resto Chile'],
    'Ene_Real': [5297, 67291, 19751, 8319, 75967],
    'Feb_Real': [5184, 47524, 29135, 8273, 104532],
    'Mar_Real': [9957, 22331, 18937, 7064, 30368],
    'Ene_Meta': [52819, 27759, 64600, 3834, 75704],
    'Feb_Meta': [19366, 20163, 40120, 2620, 42150],
    'Mar_Meta': [17444, 16683, 33715, 3643, 23029],
    'Abr_Meta': [20963, 14563, 32108, 2893, 25677]}
df = pd.DataFrame(data)

# 2. Configuración Visual Mejorada (Más grande y respirable)
plt.style.use('seaborn-v0_8-whitegrid')
fig, axes = plt.subplots(3, 2, figsize=(18, 14), facecolor='#ffffff')
axes = axes.flatten()
meses = ['Ene', 'Feb', 'Mar', 'Abr']

# Colores ajustados para mayor elegancia
color_meta = '#8e44ad'   # Morado
color_exito = '#27ae60'  # Verde vibrante
color_alerta = '#e74c3c' # Rojo vibrante
color_texto = '#2c3e50'  # Gris oscuro/azul (más suave a la vista que el negro)

# MAGIA VISUAL: Efecto de borde blanco para los textos
efecto_borde = [path_effects.withStroke(linewidth=4, foreground='white')]

for i, destino in enumerate(df['Destino']):
    ax = axes[i]
    row = df[df['Destino'] == destino].iloc[0]
    
    reales = [row['Ene_Real'], row['Feb_Real'], row['Mar_Real'], 0]
    metas = [row['Ene_Meta'], row['Feb_Meta'], row['Mar_Meta'], row['Abr_Meta']]
    x = np.arange(len(meses))
    width = 0.45
    
    colores_barras = [color_exito if r >= m else color_alerta for r, m in zip(reales[:3], metas[:3])] + ['none']
    
    # Gráficos limpios
    ax.bar(x, reales, width, color=colores_barras, alpha=0.9, edgecolor='white', linewidth=1, zorder=2)
    ax.plot(x, metas, marker='o', linestyle='-', color=color_meta, linewidth=3.5, markersize=8, zorder=3)
    
    # Aumentar límite Y drásticamente para dar espacio a los textos arriba
    max_y = max(max(reales), max(metas))
    ax.set_ylim(0, max_y * 1.35) 
    
    # Título limpio, integrado en el gráfico, alineado a la izquierda como un "Header"
    ax.set_title(destino.upper(), fontsize=16, fontweight='900', color=color_texto, loc='left', pad=15)
    
    # Textos: % de Cumplimiento
    for j in range(3):
        pct = (reales[j] / metas[j]) * 100
        # TRUCO DINÁMICO: Posicionar el texto SIEMPRE por encima del punto más alto (línea o barra)
        y_pos = max(reales[j], metas[j]) + (max_y * 0.04)
        
        ax.text(j, y_pos, f"{pct:.0f}%", ha='center', va='bottom', fontsize=12, fontweight='bold', 
                color=colores_barras[j], path_effects=efecto_borde, zorder=4)

    # Etiqueta limpia de Meta para ABRIL
    meta_abril = metas[3]
    ax.annotate(f"Target\n${meta_abril:,.0f}", 
                xy=(3, meta_abril), 
                xytext=(0, 20), textcoords="offset points", 
                ha='center', va='bottom', fontsize=12, fontweight='bold', color=color_meta, 
                path_effects=efecto_borde, zorder=4,
                arrowprops=dict(arrowstyle="-", color=color_meta, lw=1.5))
    
    # Limpieza visual de bordes innecesarios
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#bdc3c7')
    
    # Formateo de ejes
    ax.set_xticks(x)
    ax.set_xticklabels(meses, fontsize=13, fontweight='bold', color=color_texto)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda val, loc: f"${int(val/1000)}k" if val > 0 else "0"))
    ax.tick_params(axis='y', colors='#7f8c8d', length=0)
    
    # Grilla horizontal tenue al fondo
    ax.grid(axis='y', linestyle='-', color='#f1f2f6', linewidth=1.5, zorder=1)
    ax.grid(axis='x', visible=False)
    
axes[5].set_visible(False) # Ocultar el 6to recuadro vacío

# Ajustes de márgenes para separar las filas
plt.subplots_adjust(hspace=0.5, wspace=0.15)

# Leyenda global minimalista sin marco
legend_elements = [
    Patch(facecolor=color_exito, alpha=0.9, label='Ingreso Real (Logrado/Superado)'),
    Patch(facecolor=color_alerta, alpha=0.9, label='Ingreso Real (Bajo Meta)'),
    Line2D([0], [0], color=color_meta, marker='o', linestyle='-', linewidth=3.5, markersize=8, label='Meta Asignada (USD)')
]
fig.legend(handles=legend_elements, loc='lower center', ncol=3, bbox_to_anchor=(0.5, 0.02), 
           fontsize=14, frameon=False, labelcolor=color_texto)

plt.suptitle('Performance CHILE Q1: Ingresos vs Metas y Proyección Abril', 
             fontsize=24, fontweight='black', y=0.97, color=color_texto)

# Ajuste final de layout
plt.tight_layout(rect=[0, 0.06, 1, 0.94])
plt.show()