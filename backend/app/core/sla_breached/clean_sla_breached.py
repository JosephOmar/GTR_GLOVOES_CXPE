import pandas as pd

def clean_sla_breached(data: pd.DataFrame):

    data = data[data['status'] == 'CLOSED']
    
    # Convert 'creation_timestamp_local' to datetime format
    data['creation_timestamp_local'] = pd.to_datetime(data['creation_timestamp_local'], format='%B %d, %Y, %I:%M %p')
    
    # Extract the date and hour (interval) from the 'creation_timestamp_local' column
    data['date'] = data['creation_timestamp_local'].dt.date
    data['interval'] = data['creation_timestamp_local'].dt.strftime('%H:00')
    data = data.rename(columns={'stakeholder':'team', 'agent_email': 'api_email'})
    # Group by 'date', 'interval', and 'agent_email' to count the number of occurrences (chat_breached)
    data_grouped = data.groupby(['team', 'date', 'interval', 'api_email']).size().reset_index(name='chat_breached')

    data_grouped['team'] = data_grouped['team'].replace({'customer': 'Customer Tier1', 'rider' : 'Rider Tier1', 'vendor' : 'Vendor Tier1'})
    
    return data_grouped

