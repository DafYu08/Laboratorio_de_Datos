# -*- coding: utf-8 -*-
import pandas as pd
from inline_sql import sql, sql_val
import matplotlib.pyplot as plt
from matplotlib import ticker   
from matplotlib import rcParams
import seaborn as sns

#insertar carpeta que quieran vvv
carpeta = "C:/Users/DAFNE/OneDrive/Documentos/TP1/"

pbi_anual_pais_original = pd.read_csv(carpeta+"API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv")
metadata_country_original = pd.read_csv(carpeta+"Metadata_Country_API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv") #no se usa
metadata_indicator_original = pd.read_csv(carpeta+"Metadata_Indicator_API_NY.GDP.PCAP.CD_DS2_en_csv_v2_73.csv") #no se usa
lista_secciones_original = pd.read_csv(carpeta+"lista-secciones.csv")
lista_sedes_datos_original = pd.read_csv(carpeta+"lista-sedes-datos.csv")
lista_sedes_original = pd.read_csv(carpeta+"lista-sedes.csv")


#%%GQM
#pbi_anual_pais_original:problema tiene regiones que los trata como pais

problema1=sql^"""
               SELECT DISTINCT p."Country Name"
               FROM pbi_anual_pais_original AS p
               INNER JOIN metadata_country_original AS m
               ON m.region=p."Country Name";
                """
print(problema1)


problema2=sql^"""
               SELECT DISTINCT s1.sede_id,s1.sede_desc_castellano,s1.nombre_titular,s1.apellido_titular
               FROM lista_secciones_original AS s1
               INNER JOIN lista_secciones_original AS s2
               ON s1.sede_id=s2.sede_id AND s1.sede_desc_castellano=s2.sede_desc_castellano AND
               (s1.nombre_titular!=s2.nombre_titular OR s1.apellido_titular!=s2.apellido_titular)
               ORDER BY s1.sede_desc_castellano,s1.sede_id;
               """
print(problema2)

#Problema lista_sedes_datos: hay muchas redes sociales que tienen @ y no sabemos a cuál hacen referencia
problema_3 = sql^"""
                    SELECT sede_id, redes_sociales
                    FROM lista_sedes_datos_original
                    WHERE redes_sociales LIKE '@%' OR redes_sociales LIKE '%//  @%'
                """ 

#%%Limpieza de datos

#Para este TP solo nos vamos a concentrar en los paices con sedes argentinas y sus respectivoas PBIs del año 2022

pais = sql^"""
                      SELECT DISTINCT pais_iso_3 AS id_pais, UPPER(Pais_castellano) AS nombre_pais,region_geografica,"2022" AS PBI
                      FROM pbi_anual_pais_original
                      INNER JOIN lista_sedes_datos_original
                      ON pais_iso_3="Country Code"  AND "2022"  IS NOT NULL;
                     """

#Aca descartamos las sedes inactivas porque no tiene sentido contarlas
sedes = sql^"""
                   SELECT DISTINCT sede_id, pais_iso_3 AS id_pais
                   FROM lista_sedes_datos_original
                   WHERE sede_id IS NOT NULL AND estado = 'Activo';
                  """

#Si una sede tiene dos secciones con un mismo nombre, entonces
#hacen referencia a lo mismo y solo se cambia o agrega un titular
#como por ejemplo,  sede_id=ERUNI, sede_desc_castellano = Administración

#REVISAR!!!! EFILI tiene como seccion nombres de personas
#los nombres de las secciones no sin consistentes pero ni importa xq solo 
#necesitamos la cantidad por sede y dentro de una sede no repite secciones
secciones = sql^"""
                       SELECT DISTINCT sede_id, LOWER(sede_desc_castellano) AS descripcion
                       FROM lista_secciones_original
                       WHERE sede_id IS NOT NULL AND sede_desc_castellano IS NOT NULL;                 
                      """
#a continuacion todo el proceso de separar url en unidades atomicas y rescatar el tipo de red social                 
redes = sql^"""
             SELECT DISTINCT sede_id ,LOWER(redes_sociales) AS URL
             FROM lista_sedes_datos_original
             WHERE redes_sociales IS NOT NULL;
            """

#Con pandas me separa los URL en columnas
redes[['r1','r2','r3','r4','r5','r6','r7']]= redes['URL'].str.split(pat='  //  ', expand=True)
redes=redes[['sede_id','r1','r2','r3','r4','r5','r6']] #r7 es todo null y tiene una unica celda vacia

#uno las columnas de url que me dio pandas y me quedo con las no vacias
redes = sql^"""
                     SELECT DISTINCT sede_id ,r1 AS URL
                     FROM redes
                     WHERE r1 like '%_%'
                     UNION
                     SELECT DISTINCT sede_id ,r2 AS URL
                     FROM redes
                     WHERE r2 like '%_%'
                     UNION
                     SELECT DISTINCT  sede_id ,r3 AS URL
                     FROM redes
                     WHERE r3 like '%_%'
                     UNION
                     SELECT DISTINCT  sede_id ,r4 AS URL
                     FROM redes
                     WHERE r4 like '%_%'
                     UNION
                     SELECT DISTINCT  sede_id ,r5 AS URL
                     FROM redes
                     WHERE r5 like '%_%'
                     UNION
                     SELECT DISTINCT  sede_id ,r6 AS URL
                     FROM redes
                     WHERE r6 like '%_%';
                     
                  """

#confirmo que todos los url tienen '.com'
test = sql^"""
                       SELECT DISTINCT *
                       FROM redes
                       WHERE url NOT LIKE '%.com%' ;            
                      """
#print(test)

#selecciono los url y rescato el tipo de red social quedandome con la parte de adelante del .com
#los @ y los nombres de usuario sueltos decidimos ignorarlos xq no se puede identificar la red social
# 'CPABL' tiene un mail colado
tipo=sql^"""
            SELECT DISTINCT URL
            FROM redes
            WHERE  url LIKE '%.com%'  AND url NOT LIKE '%mail%'          
            """
#print(tipo)

tipo[['red','trash']]= tipo['URL'].str.split(pat='.com',n=1, expand=True)
tipo=tipo[['red']]
tipo= tipo['red'].str.split(pat='\.|//',n=2, expand=True)

#me quedo con el ultimo no null
#el primero son los que tienen http y www, el segundo alguno de los 2 y el tercero ninguno

tipo=sql^"""
                SELECT DISTINCT "2" AS red_social 
                FROM tipo
                WHERE  "2" IS NOT NULL 
                UNION
                SELECT DISTINCT "1" AS red_social 
                FROM tipo
                WHERE  "2" IS NULL AND "1" IS NOT NULL
                UNION
                SELECT DISTINCT "0" AS red_social 
                FROM tipo
                WHERE  "1" IS NULL AND "0" IS NOT NULL;
                """
print(tipo)

#por tipos_red sabemos que solo hay 6 tipos de redes sociales: facebook,twitter,instagram,youtube,linkedin y flickr

redes = sql^"""
                    SELECT DISTINCT sede_id,'Facebook' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%facebook%'
                    UNION
                    SELECT DISTINCT sede_id,'Twitter' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%twitter%'
                    UNION
                    SELECT DISTINCT sede_id,'Instagram' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%instagram%'
                    UNION
                    SELECT DISTINCT sede_id,'Youtube' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%youtube%'
                    UNION
                    SELECT DISTINCT sede_id,'Linkedin' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%linkedin%'
                    UNION
                    SELECT DISTINCT sede_id,'Flickr' AS Red_Social, URL
                    FROM redes
                    WHERE url LIKE '%flickr%'
                    
                    ORDER BY sede_id,red_social;
                   """
#print(redes)

#confirmo q son clave
test = sql^"""
                    select distinct sede_id,red_social
                    from redes
                    
                   """


#%% Data frames (ej. h)

#---------------------------------------------Ejercicio 1----------------------------------------------
cant_sedes_pais = sql^"""
                       SELECT DISTINCT p.id_pais,p.nombre_pais, count(s.id_pais) AS cant_sedes
                       FROM pais AS p
                       INNER JOIN sedes AS s
                       ON s.id_pais = p.id_pais
                       GROUP BY  p.nombre_pais, p.id_pais;
                      """

cant_secciones_pais = sql^"""
                           SELECT DISTINCT sed.id_pais,count(sec.sede_id) AS cant_secciones
                           FROM  sedes AS sed
                           LEFT OUTER JOIN secciones AS sec
                           ON sec.sede_id = sed.sede_id
                           GROUP BY sed.id_pais;
                          """


cantidades=sql^"""
                SELECT DISTINCT sed.*,cant_secciones
                FROM cant_sedes_pais AS sed
                INNER  JOIN cant_secciones_pais AS sec
                ON sed.id_pais = sec.id_pais;
                """


ejercicio_i = sql^"""
                   SELECT DISTINCT c.nombre_pais AS Pais,
                                   cant_sedes AS Sedes,
                                   ROUND(cant_secciones/cant_sedes,1) AS 'Secciones promedio',
                                   PBI AS 'PBI per cápita 2022 (U$S)'
                   FROM pais AS p
                   INNER JOIN cantidades AS c
                   ON c.id_pais = p.id_pais
                   ORDER BY sedes DESC, c.nombre_pais ASC;
                  """
ejercicio_i_informe = ejercicio_i.head()
#---------------------------------------------Ejercicio 2----------------------------------------------
pais_region_pbi = sql^"""
                           SELECT DISTINCT sed.nombre_pais, region_geografica, PBI
                           FROM cant_sedes_pais AS sed
                           INNER JOIN pais AS p
                           ON p.id_pais = sed.id_pais ;
                          """

ejercicio_ii = sql^"""
                    SELECT DISTINCT region_geografica AS 'Región geográfica',
                                    count(region_geografica) AS 'Países Con Sedes Argentinas',
                                    AVG(PBI) AS 'Promedio PBI per cápita 2022 (U$S)'
                    FROM pais_region_pbi
                    GROUP BY region_geografica
                    ORDER BY 'Promedio PBI per cápita 2022 (U$S)' DESC;
                   """

#---------------------------------------------Ejercicio 4----------------------------------------------
pais_sede= sql^"""
                    SELECT DISTINCT nombre_pais,sede_id
                    FROM pais
                    INNER JOIN sedes
                    ON pais.id_pais=sedes.id_pais;
                   """

ejercicio_iv = sql^"""
                    SELECT DISTINCT nombre_pais AS País, ps.sede_id AS Sede, red_social AS 'Red Social',URL
                    FROM pais_sede AS ps
                    INNER JOIN redes
                    ON ps.sede_id=redes.sede_id
                    ORDER BY nombre_pais,sede,red_social,url;
                   """

#---------------------------------------------Ejercicio 3----------------------------------------------
ejercicio_iii = sql^"""
                    SELECT DISTINCT País,COUNT (*) AS 'Cantidad distinta de redes sociales por pais'
                    FROM (
                        SELECT DISTINCT País,"Red Social"
                        FROM ejercicio_iv
                        )
                    GROUP BY País
                    ORDER BY "Cantidad distinta de redes sociales por pais" DESC,País;
                   """

#%% Graficos (ej. i)
#---------------------------------------------Grafico 1----------------------------------------------
ejercicio_ii = ejercicio_ii.sort_values('Países Con Sedes Argentinas')
#colorcitos = ["#ff5733", "#E0753B", "#E58606", "#FF9E00", "#99C945", "#52BCA3","#17becf", "#5D69B1", "#8e44ad"]
fig, ax = plt.subplots()    

plt.rcParams['font.family'] = 'sans-serif'                
ax.bar(data=ejercicio_ii, 
       x='Región geográfica', 
       height='Países Con Sedes Argentinas',
       edgecolor='k',
       color = "#52BCA3"
      )
ax.set_title('Cantidad de sedes por Región',fontweight='bold')                   
ax.set_xlabel('Región', fontsize='medium')   #este lo sacaria      
ax.set_ylabel('Cantidad de sedes', fontsize='medium')  
plt.xticks(fontsize = 9, rotation=25, horizontalalignment='right')
ax.set_ylim(0, 22)
                                  
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}")); 
plt.gca().set_facecolor('#FFF8DC')  
plt.gcf().set_facecolor('#F5F5DC')
plt.show()


#---------------------------------------------Grafico 2----------------------------------------------

order = pais_region_pbi.groupby('region_geografica')['PBI'].median().sort_values().index

sns.set_style("whitegrid")  # Fondo blanco con líneas de rejilla
sns.set_context("notebook")  # Ajustar el tamaño de la fuente

plt.figure(figsize=(16, 9))


ax = sns.boxplot(y="region_geografica", 
                x="PBI",  
                 data=pais_region_pbi, 
                 order = order,
                 showmeans = True,
                 meanprops=dict(marker = '*',markerfacecolor='white',markeredgecolor='k',markersize=12),
                 palette = ["#52BCA3"],
                 linewidth=1.5,  # Grosor de los bordes
                 boxprops=dict(edgecolor='black'),   # Color del borde de los boxplots
                 whiskerprops=dict(color='black'),   # Color de las líneas que representan los bigotes
                 medianprops=dict(color='black'),  # Color de la línea que representa la mediana
                 capprops=dict(color='black'))      # Color de las líneas que representan los extremos de los bigotes)

# Ajustar el tamaño de las etiquetas de las regiones
plt.xticks(fontsize=14)
plt.yticks(fontsize=14)
#plt.xticks(rotation=25, horizontalalignment='right')
ax.set_title('PBI per cápita por Región', fontsize=20, fontweight='bold')
ax.set_ylabel('Región', fontsize=18, fontweight='bold')
ax.set_xlabel('PBI per cápita 2022 (U$S)', fontsize=18, fontweight='bold')
ax.set_xlim(0,110000) 
plt.gca().set_facecolor('#FFF8DC')  # Blanco crema
plt.gcf().set_facecolor('#F5F5DC')  # Blanco crema más oscuro
ax.xaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"));

# Mostrar el boxplot
plt.show()

#---------------------------------------------Grafico 3----------------------------------------------

fig,ax= plt.subplots()
plt.rcParams['font.family'] = 'sans-serif'
ax.scatter(data=ejercicio_i,x='Sedes',y='PBI per cápita 2022 (U$S)',marker='o',edgecolor='k',color="#52BCA3")
ax.set_title('Relación entre el PBI per cápita y cantidad de sedes Argentinas por países',fontweight='bold')
ax.set_xlabel('Cantidad de sedes',fontsize='medium')
ax.set_ylabel('PBI per cápita 2022 (U$S)',fontsize='medium')
#ax.set_xlim(-1,12)
#ax.set_ylim(0,110000)
ax.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"));
plt.gca().set_facecolor('#FFF8DC')  # Blanco crema
plt.gcf().set_facecolor('#F5F5DC') 
plt.show()