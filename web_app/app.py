"""Streamlit Web Application for Multi-Agent Data Wrangler.

This module provides a user-friendly web interface for the data wrangling pipeline.
"""

import sys
from pathlib import Path

# Add project root to path - do this FIRST before any other imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import json
from typing import Optional, Any

# Page configuration
st.set_page_config(
    page_title="Multi-Agent Data Wrangler",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for styling
st.markdown(
    """
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #4F46E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #1F2937;
        margin-top: 1rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #D1FAE5;
        border-radius: 0.5rem;
        border-left: 4px solid #10B981;
    }
    .info-box {
        padding: 1rem;
        background-color: #DBEAFE;
        border-radius: 0.5rem;
        border-left: 4px solid #4F46E5;
    }
    .step-card {
        padding: 0.75rem;
        background-color: #F3F4F6;
        border-radius: 0.5rem;
        margin: 0.25rem 0;
    }
    .metric-card {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        text-align: center;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_config() -> dict:
    """Get configuration."""
    try:
        from src.common.config.loader import ConfigLoader
        loader = ConfigLoader()
        config = loader.load_config("app_config.yaml")
        return config
    except Exception:
        return {
            "app": {
                "title": "Multi-Agent Data Wrangler",
                "description": "AI-powered data wrangling pipeline",
                "theme": "light",
            },
            "display": {
                "show_flow": True,
                "show_progress": True,
                "max_preview_rows": 100,
            },
        }


def create_sample_data() -> pd.DataFrame:
    """Create sample data for demonstration."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", None, "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"],
            "age": [25, None, 35, 45, 22, 38, 29, None, 31, 27],
            "city": ["New York", "Los Angeles", "Chicago", "Houston", None, "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas"],
            "salary": [50000, 60000, 55000, 70000, 45000, None, 58000, 62000, 52000, 58000],
            "department": ["Engineering", "Marketing", "Engineering", "Sales", None, "Engineering", "Marketing", "Sales", "Engineering", "Marketing"],
        }
    )


def initialize_pipeline(failure_strategy: str = "SKIP", max_retries: int = 3):
    """Initialize the pipeline manager with all required components.
    
    Args:
        failure_strategy: Strategy for handling failures (SKIP, RETRY, ABORT, FALLBACK)
        max_retries: Maximum number of retries for RETRY strategy
    """
    from src.data_profiling.profiler import DataProfilerService
    from src.transformation.candidate_generator import CandidateGenerator
    from src.transformation.executor import TransformationExecutor
    from src.validation.validator import ValidationService
    from src.quality_scoring.scorer import QualityScorerService as ScorerImpl
    from src.ranking.ranker import RankingService as RankerImpl
    from src.ranking.policies import ImprovementPolicy
    from src.orchestrator.pipeline_manager import PipelineManager
    from src.orchestrator.agent_coordinator import AgentCoordinator
    from src.orchestrator.failure_recovery import FailureStrategy
    
    # Map string to FailureStrategy enum
    strategy_map = {
        "SKIP": FailureStrategy.SKIP,
        "RETRY": FailureStrategy.RETRY,
        "ABORT": FailureStrategy.ABORT,
        "FALLBACK": FailureStrategy.FALLBACK,
    }
    fs = strategy_map.get(failure_strategy.upper(), FailureStrategy.SKIP)
    
    # Create instances of all components
    profiler = DataProfilerService()
    transformation_engine = CandidateGenerator()
    executor = TransformationExecutor()
    validation_engine = ValidationService()
    quality_scorer = ScorerImpl()
    # RankingService requires a policy - use ImprovementPolicy
    ranking_engine = RankerImpl(policy=ImprovementPolicy())
    
    # Create combined transformation engine
    class CombinedTransformationEngine:
        def __init__(self, generator, executor):
            self.generator = generator
            self.executor = executor
        
        def generate_candidates(self, profile):
            return self.generator.generate_candidates(profile)
        
        def execute(self, data, transformation):
            return self.executor.execute(data, transformation)
        
        def reverse(self, data, transformation):
            return data
    
    transformation_combined = CombinedTransformationEngine(
        transformation_engine, executor
    )
    
    # Create coordinator with all required components
    coordinator = AgentCoordinator(
        profiler=profiler,
        transformation_engine=transformation_combined,
        validation_engine=validation_engine,
        quality_scorer=quality_scorer,
        ranking_engine=ranking_engine,
    )
    
    return PipelineManager(
        coordinator=coordinator,
        failure_strategy=fs,
        max_retries=max_retries
    )


def get_pipeline_config(max_iterations: int, enable_ranking: bool, quality_threshold: float):
    """Create pipeline config."""
    from src.common.types.pipeline import PipelineConfig
    return PipelineConfig(
        max_iterations=max_iterations,
        enable_ranking=enable_ranking,
        quality_threshold=quality_threshold,
    )


def display_pipeline_flow():
    """Display the pipeline flow diagram."""
    st.markdown("### Pipeline Flow")
    
    flow_diagram = """
    digraph {
        rankdir=LR;
        node [shape=box, style=filled, fontname="Arial"];
        
        Input [fillcolor="#DBEAFE", fontcolor="#1E40AF"];
        Profiling [fillcolor="#E0E7FF", fontcolor="#3730A3"];
        Transformation [fillcolor="#FEF3C7", fontcolor="#92400E"];
        Validation [fillcolor="#FCE7F3", fontcolor="#9D174D"];
        Quality [fillcolor="#D1FAE5", fontcolor="#065F46"];
        Ranking [fillcolor="#E5E7EB", fontcolor="#374151"];
        Output [fillcolor="#DBEAFE", fontcolor="#1E40AF"];
        
        Input -> Profiling;
        Profiling -> Transformation;
        Transformation -> Validation;
        Validation -> Quality;
        Quality -> Ranking;
        Ranking -> Output;
    }
    """
    
    try:
        st.graphviz_chart(flow_diagram)
    except Exception:
        cols = st.columns(7)
        steps = ["Input", "Profile", "Transform", "Validate", "Quality", "Rank", "Output"]
        for i, (col, step) in enumerate(zip(cols, steps)):
            with col:
                st.markdown(f"<div class='step-card'>{step}</div>", unsafe_allow_html=True)


def display_profile(profile: Any):
    """Display data profile information."""
    st.markdown("### Data Profile")
    
    if profile is None:
        st.warning("No profile data available")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", profile.row_count if hasattr(profile, 'row_count') else "N/A")
    with col2:
        st.metric("Columns", profile.column_count if hasattr(profile, 'column_count') else "N/A")
    with col3:
        st.metric("Missing (%)", f"{profile.overall_missing_percentage:.1f}%" if hasattr(profile, 'overall_missing_percentage') else "N/A")
    with col4:
        st.metric("Duplicates", profile.duplicate_rows if hasattr(profile, 'duplicate_rows') else "N/A")
    
    if hasattr(profile, 'columns') and profile.columns:
        st.markdown("#### Column Details")
        cols_info = []
        for col_name, col_profile in profile.columns.items():
            cols_info.append({
                "Column": col_name,
                "Type": col_profile.dtype,
                "Missing": f"{col_profile.null_percentage:.1f}%" if col_profile.null_percentage is not None else "N/A",
                "Unique": col_profile.unique_count,
            })
        
        if cols_info:
            st.dataframe(pd.DataFrame(cols_info), use_container_width=True)


def display_rankings(ranked_transformations: list):
    """Display ranked transformations."""
    st.markdown("### 🏆 Ranked Transformations")
    
    if not ranked_transformations:
        st.warning("No ranked transformations available")
        return
    
    rankings_data = []
    for i, ranked in enumerate(ranked_transformations[:10], 1):
        try:
            candidate = ranked.candidate
            rankings_data.append({
                "Rank": i,
                "Quality Score": f"{ranked.composite_score:.3f}" if hasattr(ranked, 'composite_score') else "N/A",
                "Reasoning": str(ranked.reasoning)[:50] + "..." if ranked.reasoning and len(str(ranked.reasoning)) > 50 else str(ranked.reasoning) if ranked.reasoning else "N/A",
            })
        except Exception as e:
            st.warning(f"Could not display ranking {i}: {str(e)}")
            continue
    
    if rankings_data:
        st.dataframe(pd.DataFrame(rankings_data), use_container_width=True)
    else:
        st.warning("No valid rankings to display")


def main():
    """Main application entry point."""
    config = get_config()
    
    st.markdown(
        f"<div class='main-header'>{config['app']['title']}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(f"*{config['app']['description']}*")
    st.markdown("---")
    
    if "pipeline_result" not in st.session_state:
        st.session_state.pipeline_result = None
    if "uploaded_data" not in st.session_state:
        st.session_state.uploaded_data = None
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        st.markdown("#### Pipeline Settings")
        enable_profiling = st.toggle("Enable Profiling", value=True)
        enable_ranking = st.toggle("Enable Ranking", value=True)
        max_iterations = st.slider("Max Iterations", 1, 50, 10)
        quality_threshold = st.slider("Quality Threshold", 0.0, 1.0, 0.8)
        
        st.markdown("---")
        st.markdown("#### Display Settings")
        show_flow = st.toggle("Show Pipeline Flow", value=config["display"].get("show_flow", True))
        max_preview = st.slider("Max Preview Rows", 10, 500, config["display"].get("max_preview_rows", 100))
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Upload Data")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help="Upload your CSV file to begin",
        )
        
        use_sample = st.button("📋 Use Sample Data", help="Use built-in sample data")
        
        if use_sample:
            st.session_state.uploaded_data = create_sample_data()
            st.success("Sample data loaded!")
    
    if uploaded_file is not None:
        try:
            st.session_state.uploaded_data = pd.read_csv(uploaded_file)
            st.success(f"Loaded: {uploaded_file.name}")
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
    
    if show_flow:
        display_pipeline_flow()
    
    if st.session_state.uploaded_data is not None:
        st.markdown("### Data Preview")
        st.dataframe(
            st.session_state.uploaded_data.head(max_preview),
            use_container_width=True,
        )
        
        st.markdown("#### Data Info")
        info_cols = st.columns(3)
        with info_cols[0]:
            st.metric("Rows", st.session_state.uploaded_data.shape[0])
        with info_cols[1]:
            st.metric("Columns", st.session_state.uploaded_data.shape[1])
        with info_cols[2]:
            st.metric("Missing Values", st.session_state.uploaded_data.isna().sum().sum())
        
        st.markdown("---")
        run_button = st.button(
            "🚀 Run Pipeline",
            type="primary",
            use_container_width=True,
        )
        
        if run_button:
            pipeline_config = get_pipeline_config(max_iterations, enable_ranking, quality_threshold)
            
            with st.spinner("Running pipeline... This may take a while."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    pipeline = initialize_pipeline()
                    
                    progress_bar.progress(20)
                    status_text.text("Profiling data...")
                    
                    result = pipeline.run(
                        data=st.session_state.uploaded_data,
                        config=pipeline_config,
                    )
                    
                    progress_bar.progress(100)
                    
                    if result.success:
                        st.session_state.pipeline_result = result
                        st.success(f"Pipeline completed in {result.execution_time_seconds:.2f} seconds!")
                    else:
                        st.error(f"Pipeline failed: {result.error}")
                        
                except Exception as e:
                    st.error(f"Error running pipeline: {str(e)}")
                    import traceback
                    st.error(traceback.format_exc())
                    progress_bar.progress(0)
    
    if st.session_state.pipeline_result is not None:
        result = st.session_state.pipeline_result
        
        st.markdown("---")
        st.markdown("### Results")
        
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Data Profile", "Transformed Data", "Quality Scores", "Rankings"]
        )
        
        with tab1:
            display_profile(result.profile)
        
        with tab2:
            st.markdown("### Transformed Data")
            if result.data is not None:
                st.dataframe(
                    result.data.head(max_preview),
                    use_container_width=True,
                )
                
                csv = result.data.to_csv(index=False)
                st.download_button(
                    label="📥 Download Transformed CSV",
                    data=csv,
                    file_name="transformed_data.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No transformed data available")
        
        with tab3:
            st.markdown("### Quality Scores")
            
            # Show quality scores from the best transformation if available
            if result.ranked_transformations:
                best = result.ranked_transformations[0]
                if best.candidate:
                    q_before = best.candidate.quality_before
                    q_after = best.candidate.quality_after
                    
                    if q_before:
                        st.markdown("#### Original Data Quality")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Completeness", f"{q_before.completeness:.3f}" if q_before.completeness else "N/A")
                        with col2:
                            st.metric("Validity", f"{q_before.validity:.3f}" if q_before.validity else "N/A")
                        with col3:
                            st.metric("Uniqueness", f"{q_before.uniqueness:.3f}" if q_before.uniqueness else "N/A")
                        with col4:
                            st.metric("Consistency", f"{q_before.consistency:.3f}" if q_before.consistency else "N/A")
                    
                    if q_after:
                        st.markdown("#### Transformed Data Quality")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Completeness", f"{q_after.completeness:.3f}" if q_after.completeness else "N/A")
                        with col2:
                            st.metric("Validity", f"{q_after.validity:.3f}" if q_after.validity else "N/A")
                        with col3:
                            st.metric("Uniqueness", f"{q_after.uniqueness:.3f}" if q_after.uniqueness else "N/A")
                        with col4:
                            st.metric("Consistency", f"{q_after.consistency:.3f}" if q_after.consistency else "N/A")
                        
                        # Show improvement
                        if q_before and q_after:
                            st.markdown("#### Quality Improvement")
                            completeness_imp = (q_after.completeness or 0) - (q_before.completeness or 0)
                            validity_imp = (q_after.validity or 0) - (q_before.validity or 0)
                            uniqueness_imp = (q_after.uniqueness or 0) - (q_before.uniqueness or 0)
                            consistency_imp = (q_after.consistency or 0) - (q_before.consistency or 0)
                            
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Completeness Δ", f"{completeness_imp:+.3f}")
                            with col2:
                                st.metric("Validity Δ", f"{validity_imp:+.3f}")
                            with col3:
                                st.metric("Uniqueness Δ", f"{uniqueness_imp:+.3f}")
                            with col4:
                                st.metric("Consistency Δ", f"{consistency_imp:+.3f}")
            else:
                st.info("No quality scores available")
        
        with tab4:
            display_rankings(result.ranked_transformations)
        
        st.markdown("### Download Report")
        
        # Build comprehensive report
        report = {
            "success": result.success,
            "execution_time_seconds": result.execution_time_seconds,
        }
        
        # Add profile data
        if result.profile:
            report["data_profile"] = {
                "row_count": result.profile.row_count,
                "column_count": result.profile.column_count,
                "overall_missing_percentage": result.profile.overall_missing_percentage,
                "duplicate_rows": result.profile.duplicate_rows,
                "columns": {}
            }
            for col_name, col_profile in result.profile.columns.items():
                report["data_profile"]["columns"][col_name] = {
                    "dtype": col_profile.dtype,
                    "inferred_type": col_profile.inferred_type,
                    "null_count": col_profile.null_count,
                    "null_percentage": col_profile.null_percentage,
                    "unique_count": col_profile.unique_count,
                    "min_value": col_profile.min_value,
                    "max_value": col_profile.max_value,
                    "mean": col_profile.mean,
                }
        
        # Add ranked transformations
        if result.ranked_transformations:
            report["ranked_transformations"] = []
            for ranked in result.ranked_transformations[:20]:
                report["ranked_transformations"].append({
                    "rank": len(report["ranked_transformations"]) + 1,
                    "transformation_id": ranked.candidate.transformation.id,
                    "transformation_type": ranked.candidate.transformation.type.value,
                    "target_columns": ranked.candidate.transformation.target_columns,
                    "description": ranked.candidate.transformation.description,
                    "composite_score": ranked.composite_score,
                    "reasoning": ranked.reasoning,
                    "validation_passed": ranked.candidate.validation_result.passed,
                    "quality_before": ranked.candidate.quality_before,
                    "quality_after": ranked.candidate.quality_after,
                })
        
        report_json = json.dumps(report, indent=2, default=str)
        st.download_button(
            label="📥 Download JSON Report",
            data=report_json,
            file_name="pipeline_report.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()
