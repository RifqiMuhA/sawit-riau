import os
import glob
import re

emoji_map = {
    '"👥"': '"fa-solid fa-users"',
    '"✅"': '"fa-solid fa-check"',
    '"📊"': '"fa-solid fa-chart-simple"',
    '"🎯"': '"fa-solid fa-bullseye"',
    '"🌾"': '"fa-solid fa-seedling"',
    '"📉"': '"fa-solid fa-arrow-trend-down"',
    '"🔔"': '"fa-solid fa-bell"',
    '"⚠️"': '"fa-solid fa-triangle-exclamation"',
    '"🏆"': '"fa-solid fa-trophy"',
    '"💰"': '"fa-solid fa-coins"',
    '"🟢"': '"fa-solid fa-circle-check"',
    '"🟡"': '"fa-solid fa-circle-exclamation"',
    '"🔴"': '"fa-solid fa-circle-xmark"',
}

kpi_func = '''def _kpi(value, label, icon_class, variant):
    return html.Div([
        html.I(className=f"{icon_class} mini-kpi-icon", style={"marginBottom": "8px"}),
        html.Div(label, className="mini-kpi-label"),
        html.Div(value, className="mini-kpi-value"),
    ], className=f"mini-kpi-card {variant}")'''

for file in glob.glob("d:/Kuliah/STIS/Semester 6/4 - Teknologi Perekayasaan Data/Project/dashboard/pages/*.py"):
    if file.endswith("home.py") or file.endswith("r_sawit.py"):
        continue
        
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
        
    for emoji, fa in emoji_map.items():
        content = content.replace(emoji, fa)
        
    # Replace the def _kpi function
    content = re.sub(r'def _kpi\(value, label, icon, variant\):\n\s+return html\.Div\(\[\n\s+html\.Div\(icon, style=\{"fontSize": "22px", "marginBottom": "6px"\}\),\n\s+html\.Div\(value, className="kpi-value"\),\n\s+html\.Div\(label, className="kpi-label"\),\n\s+\], className=f"kpi-card \{variant\}"\)', kpi_func, content)
    
    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)

print("Done")
