from Extraction import postgres_conn, Extract



def transform():
    pg_conn = postgres_conn()

    infant_death_rate = Extract(pg_conn, 'infant_death_rate')
    countries = Extract(pg_conn, 'Countries')
    paises = Extract(pg_conn, 'Paises')
    population = Extract(pg_conn, 'Population')
    life_expectancy = Extract(pg_conn, 'life_expectancy')
    
    
    #COUNTRIES
    #Dropping '_.' characters on 'Country Code' column
    countries['Country Code']= countries['Country Code'].str.replace('_.','')
    
    #Replacing of '*' character by 'i', droping all non-letter characters and dropping spaces.
    countries['Continent'] = countries['Continent'].str.replace('*','i')
    countries['Continent'] = countries['Continent'].str.replace('/', '')
    countries['Continent'] = countries['Continent'].str.replace(r'\s', '', regex=True)

    
    #PAISES
    #Substitution of '4' by 'a'
    for idx, pais in enumerate(paises['Pais']):
        if pais.startswith('4'):
            paises.at[idx, 'Pais'] = 'A' + pais[1:]
        else: paises.at[idx, 'Pais']= pais.replace('4','a')
       
    #Replacing all non-letter characters
    paises= paises.applymap(lambda x: ''.join(char for char in str(x) if char.isalpha()))
    
    #POPULATION
    #Replacing of '4' by 'a'
    for idx, Country in enumerate(population['Country']):
        if Country.startswith('4'):
            population.at[idx, 'Country'] = 'A' + Country[1:]
        else: population.at[idx, 'Country']= Country.replace('4','a')
    
    #Remove '='
    population['Country'] = population['Country'].str.replace('=', '')
    
    #LIFE_EXPECTANCY: Drop useless columns
    life_expectancy=life_expectancy.drop(columns=['Unnamed: 0', 'South Africa', 'Unnamed: 4'])
    
    pg_conn.close()
    
    return countries, paises, population, life_expectancy, infant_death_rate


    
     
    
    
    
    
