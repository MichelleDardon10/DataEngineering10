import pandas as pd
from datetime import datetime

@transformer
def comprehensive_quality_checks(output, *args, **kwargs):
    """
    Aplicar validaciones de calidad según especificación del proyecto
    """
    if output['data'].empty:
        print("No hay datos para validar")
        return output
    
    df = output['data'].copy()
    print(f"Validando {len(df)} registros...")
    
    def calculate_quality_score(row):
        base_score = 100
        deductions = 0
        issues = []
        
        # 1. SCHEMA & COMPLETENESS (40 puntos)
        required_fields = ['trip_id', 'bike_id', 'start_time', 'end_time', 
                          'start_station_id', 'end_station_id', 'trip_duration', 'bike_type']
        
        for field in required_fields:
            if field not in row or pd.isna(row[field]) or row[field] in ['', None]:
                deductions += 10
                issues.append(f'missing_{field}')
        
        # 2. DATA VALIDITY (40 puntos)
        try:
            start_time = pd.to_datetime(row['start_time'])
            end_time = pd.to_datetime(row['end_time'])
            
            if start_time > end_time:
                deductions += 25
                issues.append('invalid_time_sequence')
            
            # Verificar duración calculada vs proporcionada
            calculated_duration = (end_time - start_time).total_seconds()
            provided_duration = float(row['trip_duration'])
            
            if abs(calculated_duration - provided_duration) > 60:
                deductions += 15
                issues.append('duration_mismatch')
                
        except:
            deductions += 25
            issues.append('time_parsing_error')
        
        # Validar rider_age
        try:
            rider_age = float(row.get('rider_age', 0))
            if rider_age < 16 or rider_age > 100:
                deductions += 20
                issues.append('invalid_age')
        except:
            deductions += 10
            issues.append('invalid_age_format')
        
        # 3. BUSINESS LOGIC (20 puntos)
        try:
            trip_duration = float(row['trip_duration'])
            if trip_duration < 60 or trip_duration > 86400:  # <1min o >24h
                deductions += 15
                issues.append('invalid_duration')
            
            # Misma estación con duración > 1 hora
            if (str(row.get('start_station_id')) == str(row.get('end_station_id')) and 
                trip_duration > 3600):
                deductions += 5
                issues.append('same_station_long_duration')
                
        except:
            deductions += 15
            issues.append('duration_calculation_error')
        
        # Calcular score final
        final_score = max(0, (base_score - deductions))
        
        return pd.Series({
            'quality_score': final_score,
            'quality_issues': issues,
            'is_valid_quality': final_score >= 60
        })
    
    # Aplicar scoring
    quality_data = df.apply(calculate_quality_score, axis=1)
    
    # Combinar con datos originales
    for col in quality_data.columns:
        df[col] = quality_data[col]
    
    output['data'] = df
    print(f"Validación completada. Score promedio: {df['quality_score'].mean():.1f}")
    return output