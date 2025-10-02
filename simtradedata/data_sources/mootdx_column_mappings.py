# Mootdx财务数据列名映射表
#
# 这个文件包含对mootdx未命名列的推断性命名
# 确定性等级: 高(80%+) | 中(50-80%) | 低(<50%)

# 高确定性推断 (基于数据特征和上下文分析)
HIGH_CONFIDENCE_MAPPINGS = {
    "col323": {
        "name": "企业自由现金流总额(元)",
        "confidence": "高",
        "basis": '紧跟"每股企业自由现金流"，数值特征匹配：col323 ≈ 每股企业自由现金流 * 总股本',
        "non_zero_rate": 1.0,
        "avg_value": 110227,
        "unit": "元",
    },
    "col324": {
        "name": "股东自由现金流总额(元)",
        "confidence": "高",
        "basis": '紧跟"每股股东自由现金流"，数值特征匹配',
        "non_zero_rate": 0.757,
        "avg_value": 17064,
        "unit": "元",
    },
    "col325": {
        "name": "是否分红标志",
        "confidence": "高",
        "basis": "只有0和1两个值，17.6%的股票为1",
        "non_zero_rate": 0.176,
        "possible_values": [0, 1],
        "unit": "标志位",
    },
    "col326": {
        "name": "分红金额(元)",
        "confidence": "高",
        "basis": "只有col325=1的股票才有值，与分红标志强关联",
        "non_zero_rate": 0.176,
        "avg_value": 69237062,
        "unit": "元",
    },
}

# 中等确定性推断
MEDIUM_CONFIDENCE_MAPPINGS = {
    "col327": {
        "name": "市盈率_扩展",
        "confidence": "中",
        "basis": "数值范围0-100，均值40，可能是市盈率或其他倍数指标",
        "non_zero_rate": 0.889,
        "avg_value": 40,
        "unit": "倍",
    },
    "col328": {
        "name": "未知金额指标1",
        "confidence": "中",
        "basis": "数值较大，均值26万，可能是某种金额指标",
        "non_zero_rate": 0.739,
        "avg_value": 263647,
        "unit": "元",
    },
    "col329": {
        "name": "未知比率指标1",
        "confidence": "中",
        "basis": "数值较小，0-30范围，可能是百分比、倍数或年限",
        "non_zero_rate": 0.926,
        "avg_value": 6,
        "unit": "未知",
    },
}

# 低确定性推断 (数据不足，无法准确推断)
LOW_CONFIDENCE_MAPPINGS = {
    "col330": {
        "name": "特殊行业指标1",
        "confidence": "低",
        "basis": "非零率仅0.2%，可能是特定行业字段",
        "non_zero_rate": 0.002,
    },
    "col331": {
        "name": "特殊行业指标2",
        "confidence": "低",
        "basis": "非零率仅0.2%，可能是特定行业字段",
        "non_zero_rate": 0.002,
    },
}

# 合并所有映射
ALL_MAPPINGS = {
    **HIGH_CONFIDENCE_MAPPINGS,
    **MEDIUM_CONFIDENCE_MAPPINGS,
    **LOW_CONFIDENCE_MAPPINGS,
}


def get_inferred_column_name(original_name: str, confidence_level: str = "high") -> str:
    """
    获取推断的列名

    Args:
        original_name: 原始列名（如'col323'）
        confidence_level: 确定性等级筛选 ('high', 'medium', 'low', 'all')

    Returns:
        str: 推断的列名，如果没有找到或不符合确定性要求则返回原名
    """
    if not original_name.startswith("col"):
        return original_name

    mapping = ALL_MAPPINGS.get(original_name)
    if not mapping:
        return original_name

    # 检查确定性等级
    confidence_map = {"高": "high", "中": "medium", "低": "low"}
    col_confidence = confidence_map.get(mapping["confidence"], "low")

    allowed_levels = {
        "high": ["high"],
        "medium": ["high", "medium"],
        "low": ["high", "medium", "low"],
        "all": ["high", "medium", "low"],
    }

    if col_confidence in allowed_levels.get(confidence_level, ["high"]):
        return mapping["name"]

    return original_name


def rename_columns_with_inference(df, confidence_level: str = "high") -> dict:
    """
    为DataFrame生成推断的列名映射

    Args:
        df: pandas DataFrame
        confidence_level: 确定性等级 ('high', 'medium', 'low', 'all')

    Returns:
        dict: {原始列名: 推断列名} 的映射字典
    """
    rename_map = {}

    for col in df.columns:
        if str(col).startswith("col"):
            inferred_name = get_inferred_column_name(col, confidence_level)
            if inferred_name != col:
                rename_map[col] = inferred_name

    return rename_map


def get_column_metadata(column_name: str) -> dict:
    """
    获取列的完整元数据

    Args:
        column_name: 列名

    Returns:
        dict: 列的元数据信息，包括推断名称、确定性、依据等
    """
    if column_name in ALL_MAPPINGS:
        return ALL_MAPPINGS[column_name]
    return {}
