# Worker 9: Web UI Module

**Directory**: `web_app/`  
**Task File**: `.cline/tasks/task_009_web_ui.md`

## Context
You are implementing the Web UI Module. This provides a user-friendly interface for the data wrangling pipeline using Streamlit.

## Prerequisites
This module depends on all other modules being working:
- **Orchestrator** - for running the pipeline
- **All pipeline modules** - Common, Data Profiling, Transformation, Validation, Quality Scoring, Ranking

## Your Task
Read the following files before implementing:
- `.cline/tasks/task_009_web_ui.md` - Detailed task requirements
- `config/pipeline.yaml` - Pipeline configuration

## What to Implement

### 1. Main Application (web_app/app.py)

Create a Streamlit application with:

**A. Header & Title**
- Set page title "Multi-Agent Data Wrangler"
- Add custom CSS for styling
- Add app description

**B. File Upload Section**
- Use `st.file_uploader` for CSV upload
- Add "Use Sample Data" button
- Show uploaded file info

**C. Pipeline Flow Visualization**
- Use `graphviz` to show pipeline flow:
  ```
  Input → Profiling → Transformation → Validation → Quality → Ranking → Output
  ```
- Use st.columns to show steps in flow

**D. Configuration Options**
- Allow user to toggle pipeline steps
- Set max candidates
- Set quality weights

**E. Run Pipeline Button**
- Use `st.button` with emoji 🚀
- Show progress with `st.progress` and `st.spinner`
- Display current step

**F. Results Display**
- Use `st.tabs` for organized results:
  - **Data Profile Tab**: Show profile stats
  - **Transformed Data Tab**: Show DataFrame with `st.dataframe`
  - **Quality Scores Tab**: Show metrics with `st.metric`
  - **Rankings Tab**: Show ranked transformations

**G. Download Section**
- `st.download_button` for transformed CSV
- `st.download_button` for report JSON

### 2. Dependencies (web_app/requirements.txt)
```
streamlit>=1.28
pandas>=2.0
graphviz
```

### 3. Configuration (config/app_config.yaml)
```yaml
app:
  title: "Multi-Agent Data Wrangler"
  description: "AI-powered data wrangling pipeline"
  theme: "light"

display:
  show_flow: true
  show_progress: true
  max_preview_rows: 100
```

### 4. Optional: Vercel Config (vercel.json)
For one-click deployment to Vercel.

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `web_app/` and `config/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **USE TYPE HINTS**: All code should have proper Python type hints where possible
5. **IDEMPOTENT**: Code must be safely re-runnable

## Code Structure Example

```python
import streamlit as st
import pandas as pd
from src.orchestrator.pipeline_manager import PipelineManager
from src.common.config.loader import ConfigLoader

# Page config
st.set_page_config(page_title="Data Wrangler", layout="wide")

# Title
st.title("🔧 Multi-Agent Data Wrangler")

# Sidebar
with st.sidebar:
    st.header("Configuration")
    max_candidates = st.slider("Max Candidates", 10, 100, 50)

# Main content
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.dataframe(df)

# Pipeline flow
st.graphviz_chart("""
digraph {
    Input -> Profiling -> Transformation -> Validation -> Quality -> Ranking -> Output
}
""")

# Run button
if st.button("🚀 Run Pipeline"):
    with st.spinner("Running pipeline..."):
        # Run pipeline
        result = run_pipeline(df, config)
        
        # Show results
        st.success("Pipeline complete!")
        st.dataframe(result.data)
```

## Acceptance Criteria
- [ ] File upload accepts CSV files
- [ ] Pipeline flow diagram is displayed
- [ ] Run button executes pipeline
- [ ] Progress is shown during execution
- [ ] Results tab shows profile, transformed data, quality, rankings
- [ ] Download buttons work for output
- [ ] Works locally with `streamlit run web_app/app.py`
- [ ] Can be deployed to Vercel/Streamlit Cloud
