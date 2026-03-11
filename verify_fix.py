
import pandas as pd

# Mock data simulating the collision for eduardo
# BOTTOM and DUO_CARRY both map to ADC
data = {
    'persona': ['eduardo', 'eduardo', 'eduardo'],
    'role': ['BOTTOM', 'DUO_CARRY', 'TOP'],
    'champion': ['Ezreal', 'Ezreal', 'Garen'],
    'matches': [10, 5, 2],
    'wins': [6, 3, 1]
}
df = pd.DataFrame(data)

role_map = {
    'TOP': 'Top',
    'JUNGLE': 'Jungla',
    'MIDDLE': 'Mid',
    'BOTTOM': 'ADC',
    'UTILITY': 'Support',
    'CARRY': 'ADC',
    'SUPPORT': 'Support',
    'DUO_CARRY': 'ADC',
    'DUO_SUPPORT': 'Support',
    'DUO': 'Support'
}

# Apply identical logic to analisis.py
df['role'] = df['role'].map(lambda x: role_map.get(x, x))
df_grouped = df.groupby(['persona', 'role', 'champion'], as_index=False).agg({
    'matches': 'sum',
    'wins': 'sum'
})

print("Grouped Data:")
print(df_grouped)

# Check for duplicates in (persona, role, champion) which form the IDs
duplicates = df_grouped.duplicated(subset=['persona', 'role', 'champion']).any()
print(f"\nDuplicates detected: {duplicates}")

expected_ezreal_matches = 15
actual_ezreal_matches = df_grouped[(df_grouped['role'] == 'ADC') & (df_grouped['champion'] == 'Ezreal')]['matches'].iloc[0]

if not duplicates and actual_ezreal_matches == expected_ezreal_matches:
    print("\nVERIFICATION SUCCESSFUL: Data is correctly grouped and IDs will be unique.")
else:
    print("\nVERIFICATION FAILED: Issues remain in data grouping.")
