# feature_pipeline.py
from __future__ import annotations
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from sklearn.model_selection import train_test_split

LOG = logging.getLogger("FeaturePipeline")

@dataclass(frozen=True)
class PipelineConfig:
    input_path: Path
    output_dir: Path
    validation_split: float = 0.2
    batch_size: int = 10_000
    max_workers: int = 4
    dtype_map: Dict[str, str] = None
    text_encoders: List[str] = None

class DataFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    FEATHER = "feather"

class FeatureType(Enum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    TEXT = "text"
    DATETIME = "datetime"

class FeaturePipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self._dtype_map = config.dtype_map or {}
        self._encoders = config.text_encoders or []
        self._executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self._feature_metadata = {}
        self._column_stats = {}

    def run(self) -> Dict[str, pd.DataFrame]:
        """Execute full preprocessing pipeline"""
        LOG.info(f"Starting pipeline for {self.config.input_path}")
        
        raw_data = self._load_data()
        cleaned_data = self._clean_data(raw_data)
        transformed_data = self._transform_features(cleaned_data)
        validated_data = self._validate_dataset(transformed_data)
        final_data = self._optimize_dtypes(validated_data)
        
        train_df, test_df = self._split_dataset(final_data)
        self._save_artifacts(train_df, test_df)
        
        LOG.info(f"Pipeline completed. Artifacts saved to {self.config.output_dir}")
        return {"train": train_df, "test": test_df}

    def _load_data(self) -> pd.DataFrame:
        """Load raw data with format detection"""
        fmt = self._detect_format(self.config.input_path)
        
        loaders = {
            DataFormat.CSV: pd.read_csv,
            DataFormat.PARQUET: pd.read_parquet,
            DataFormat.FEATHER: pd.read_feather
        }
        
        try:
            df = loaders[fmt](self.config.input_path, dtype=self._dtype_map)
            LOG.debug(f"Loaded {len(df)} rows from {self.config.input_path}")
            return df
        except Exception as e:
            raise DataLoadError(f"Failed to load {fmt.value} data: {str(e)}")

    def _detect_format(self, path: Path) -> DataFormat:
        """Auto-detect file format"""
        ext = path.suffix.lower()
        if ext == ".csv":
            return DataFormat.CSV
        elif ext == ".parquet":
            return DataFormat.PARQUET
        elif ext in (".fea", ".feather"):
            return DataFormat.FEATHER
        raise UnsupportedFormatError(f"Unknown file format: {ext}")

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data cleaning operations"""
        df = df.pipe(self._drop_duplicates)
        df = df.pipe(self._handle_missing_values)
        df = df.pipe(self._remove_outliers)
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove exact duplicate rows"""
        initial_count = len(df)
        df = df.drop_duplicates()
        LOG.info(f"Removed {initial_count - len(df)} duplicate rows")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Smart missing value handling"""
        for col in df.columns:
            if df[col].dtype == "object":
                df[col].fillna("missing", inplace=True)
            elif is_numeric_dtype(df[col]):
                df[col].fillna(df[col].median(), inplace=True)
        return df

    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """IQR-based outlier removal"""
        for col in df.select_dtypes(include=np.number).columns:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            df = df[(df[col] >= q1 - 1.5*iqr) & (df[col] <= q3 + 1.5*iqr)]
        return df

    def _transform_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Feature engineering pipeline"""
        df = df.pipe(self._normalize_features)
        df = df.pipe(self._encode_text_features)
        df = df.pipe(self._generate_time_features)
        return df

    def _normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Z-score normalization for numeric features"""
        numeric_cols = df.select_dtypes(include=np.number).columns
        for col in numeric_cols:
            if df[col].std() > 0:
                df[col] = (df[col] - df[col].mean()) / df[col].std()
                self._column_stats[col] = {"mean": df[col].mean(), "std": df[col].std()}
        return df

    def _encode_text_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parallel text feature encoding"""
        futures = []
        for col in df.select_dtypes(include="object").columns:
            futures.append(self._executor.submit(self._apply_encoding, df[col]))
        
        for future, col in zip(futures, df.select_dtypes(include="object").columns):
            df[col] = future.result()
        
        return df

    def _apply_encoding(self, series: pd.Series) -> pd.Series:
        """Apply configured text encoding strategies"""
        if "hash" in self._encoders:
            return series.apply(lambda x: hash(x) % 1000)
        elif "frequency" in self._encoders:
            freq_map = series.value_counts(normalize=True)
            return series.map(freq_map)
        return pd.get_dummies(series, drop_first=True)

    def _generate_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """DateTime feature engineering"""
        dt_cols = df.select_dtypes(include="datetime").columns
        for col in dt_cols:
            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_dayofweek"] = df[col].dt.dayofweek
            df[f"{col}_hour"] = df[col].dt.hour
        return df.drop(columns=dt_cols)

    def _validate_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data quality checks"""
        if df.isnull().sum().sum() > 0:
            raise DataValidationError("Null values detected after cleaning")
        
        numeric_cols = df.select_dtypes(include=np.number).columns
        for col in numeric_cols:
            if df[col].std() == 0:
                LOG.warning(f"Constant feature detected: {col}")
        
        return df

    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reduce memory footprint"""
        for col in df.select_dtypes(include="integer"):
            df[col] = pd.to_numeric(df[col], downcast="integer")
        for col in df.select_dtypes(include="float"):
            df[col] = pd.to_numeric(df[col], downcast="float")
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].astype("category")
        return df

    def _split_dataset(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Stratified train-test split"""
        stratify_col = df.select_dtypes(include="category").columns[0] if \
            df.select_dtypes(include="category").any().any() else None
        
        return train_test_split(
            df,
            test_size=self.config.validation_split,
            stratify=stratify_col
        )

    def _save_artifacts(self, train_df: pd.DataFrame, test_df: pd.DataFrame) -> None:
        """Save processed datasets"""
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        
        train_path = self.config.output_dir / "train.parquet"
        test_path = self.config.output_dir / "test.parquet"
        
        train_df.to_parquet(train_path)
        test_df.to_parquet(test_path)
        
        LOG.info(f"Saved training data ({len(train_df)} rows) to {train_path}")
        LOG.info(f"Saved testing data ({len(test_df)} rows) to {test_path}")

class PipelineError(Exception):
    """Base pipeline exception"""

class DataLoadError(PipelineError):
    """Data loading failure"""

class UnsupportedFormatError(PipelineError):
    """Unsupported file format"""

class DataValidationError(PipelineError):
    """Data quality check failure"""

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    config = PipelineConfig(
        input_path=Path("data/raw_dataset.csv"),
        output_dir=Path("data/processed"),
        dtype_map={"user_id": "string", "timestamp": "datetime64[ns]"},
        text_encoders=["frequency"]
    )
    
    pipeline = FeaturePipeline(config)
    artifacts = pipeline.run()
