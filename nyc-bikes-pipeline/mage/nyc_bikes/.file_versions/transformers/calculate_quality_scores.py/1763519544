import pandas as pd

@transformer
def calculate_quality_scores(output, *args, **kwargs):
    """
    Calcular métricas agregadas de calidad
    """
    if output['data'].empty:
        return output
    
    df = output['data']
    
    # Calcular bands de calidad
    excellent = len(df[df['quality_score'] >= 90])
    good = len(df[(df['quality_score'] >= 75) & (df['quality_score'] < 90)])
    fair = len(df[(df['quality_score'] >= 60) & (df['quality_score'] < 75)])
    poor = len(df[df['quality_score'] < 60])
    
    quality_stats = {
        'quality_bands': {
            'EXCELLENT': excellent,
            'GOOD': good,
            'FAIR': fair,
            'POOR': poor
        },
        'total_records': len(df),
        'valid_records': len(df[df['is_valid_quality']]),
        'invalid_records': len(df[~df['is_valid_quality']]),
        'avg_score': df['quality_score'].mean(),
        'timestamp': pd.Timestamp.now().isoformat()
    }
    
    print(f"Resumen de calidad: {quality_stats}")
    
    output['quality_stats'] = quality_stats
    return output