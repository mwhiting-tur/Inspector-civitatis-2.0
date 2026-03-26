import re
import pandas as pd
import os

# 1. Los archivos TXT que descargaste manualmente
archivos_sitemap = [
    "gyg\sitemap-activity-0.txt",
    "gyg\sitemap-activity-1.txt",
    "gyg\sitemap-activity-2.txt",
    "gyg\sitemap-activity-3.txt",
    "gyg\sitemap-activity-4.txt",
    "gyg\sitemap-activity-5.txt",
    "gyg\sitemap-activity-6.txt"
]

# 2. Las ciudades de Brasil (añade todas las que obtuviste en tu HTML anterior)
ciudades_brasil = [
    "maxaranguape-l133602", "natal-l2112", "sao-miguel-do-gostoso-l149207", 
    "galinhos-l145686", "mandacaru-l175773", "santo-amaro-do-maranhao-l215939", 
    "barreirinhas-l105727", "atins-l158343", "arraial-del-cabo-l95354", 
    "parati-mirim-l214877", "rio-de-janeiro-l9", "jose-goncalves-l213392", 
    "penedo-brasil-l246877", "paraty-l1845", "saquarema-l103071", 
    "petropolis-l1831", "nova-friburgo-l156957", "guapimirim-l163965", 
    "buzios-l1832", "duque-de-caxias-l101373", "cabo-frio-l121720", 
    "mangaratiba-l206554", "araruama-l100623", "vassouras-l151426", 
    "sao-cristovao-l198410", "tres-rios-l240147", "mambucaba-l249434", 
    "niteroi-l34109", "barra-do-pirai-l210450", "angra-dos-reis-l88183", 
    "teresopolis-l154917", "cachoeiras-de-macacu-l208627", "blumenau-l100287", 
    "itajai-l32559", "cachoeira-do-bom-jesus-l214780", "florianopolis-l246", 
    "santo-amaro-da-imperatriz-l240306", "pomerode-l232971", "penha-l206944", 
    "canasvieiras-l229495", "navegantes-l101196", "bombinhas-l106807", 
    "balneario-camboriu-l32252", "barra-da-lagoa-l230903", "brasilia-l144326", 
    "aracaju-l100614", "sao-joao-del-rei-l152587", "ouro-preto-l155573", 
    "andradas-l244383", "congonhas-minas-gerais-l266117", "januaria-l250975", 
    "belo-horizonte-l32265", "passa-quatro-l222250", "itamonte-l259498", 
    "cataguases-l154748", "salvaterra-l181818", "soure-brasil-l181819", 
    "anajas-l215892", "santarem-l121340", "belem-brasil-l32400", 
    "alter-do-chao-l148407", "guarulhos-l150643", "botucatu-l155076", 
    "san-francisco-javier-l218524", "sao-vicente-l127333", "cubatao-l151955", 
    "praia-grande-sao-paulo-brasil-l188927", "sao-paulo-l384", "campos-do-jordao-l95750", 
    "canguera-l207225", "capivari-l208929", "sao-sebastiao-l156225", 
    "ubatuba-l192258", "guaruja-l127328", "sao-bernardo-do-campo-l151674", 
    "cunha-l237291", "santos-l1708", "ilhabela-l103698", 
    "guaratingueta-l222795", "aparecida-l112622", "campinas-l1696", 
    "holambra-l176262", "sao-roque-sao-paulo-l214835", "sao-jose-dos-campos-l152479", 
    "pindamonhangaba-l157698", "bertioga-l221175", "leme-sao-paulo-l156945", 
    "braganca-paulista-l154865", "porto-jofre-l123795", "cuiaba-l32316", 
    "chapada-dos-guimaraes-l222401", "cabedelo-l206765", "coqueirinho-l230016", 
    "joao-pessoa-l116745", "porto-alegre-l32362", "cambara-do-sul-l266083", 
    "sao-leopoldo-l152104", "novo-hamburgo-l153722", "canela-l117155", 
    "carlos-barbosa-l216889", "dois-irmaos-l252597", "gramado-l32317", 
    "flores-da-cunha-l154785", "garibaldi-brasil-l216888", "bento-goncalves-l117160", 
    "vale-dos-vinhedos-l239643", "guarajuba-l128588", "sao-felix-l143898", 
    "mangue-seco-l123911", "cachoeira-l143899", "mucuge-l214116", 
    "imbassai-l143044", "coroa-vermelha-l213390", "arraial-d-ajuda-l215362", 
    "palmeiras-l208614", "caete-acu-l234553", "itaparica-l126788", 
    "morro-de-sao-paulo-l116740", "trancoso-brasil-l211130", "caravelas-l126778", 
    "caraiva-l209020", "ilheus-l126783", "porto-seguro-l126793", 
    "guinea-l214769", "ibicoara-l233877", "santo-amaro-l103068", 
    "salvador-brasil-l1430", "lencois-l32549", "cairu-l206026", 
    "mata-de-sao-joao-l215935", "itacare-l143043", "marau-l214111", 
    "andarai-bahia-l240274", "valenca-l126828", "acu-da-torre-l214898", 
    "parnaiba-l194334", "san-luis-l105732", "pirenopolis-l248251", 
    "iranduba-l215937", "presidente-figueiredo-l109827", "manaos-l917", 
    "careiro-da-varzea-l185149", "tabatinga-l256569", "autazes-l214115", 
    "careiro-l243682", "jericoacoara-l105737", "fortaleza-l1863", 
    "caponga-l141896", "aracati-l214102", "mango-seco-l246510", 
    "porto-das-dunas-l146529", "prea-l235717", "mundau-l141881", 
    "canoa-quebrada-l253023", "beberibe-l141901", "paraipaba-l215936", 
    "lagoinha-l141886", "caucaia-l208637", "iracema-l212359", 
    "trairi-l214105", "aquiraz-l206443", "miranda-l248292", 
    "bonito-l32642", "campo-grande-l101880", "matelandia-l214878", 
    "curitiba-l1849", "paranagua-l265980", "sao-jose-dos-pinhais-l212800", 
    "antonina-l232004", "foz-de-iguazu-l497", "morretes-l117165", 
    "guaratuba-l102534", "marechal-deodoro-l193612", "sao-miguel-dos-milagres-l98856", 
    "jequia-da-praia-l143083", "maceio-l2064", "maragogi-l122555", 
    "piacabucu-l252042", "barra-de-sao-miguel-l203216", "roteiro-l208321", 
    "paripueira-l193609", "igarassu-l143033", "maracaipe-l242061", 
    "jaboatao-dos-guararapes-l190645", "tamandare-ramirez-forte-l205863", "olinda-l1837", 
    "cabo-de-consolacion-l100314", "ipojuca-l215891", "porto-de-galinhas-l32612", 
    "recife-l1836", "aracatiba-l176554", "villa-do-abraao-l176580", 
    "parnaioca-l219222"
]

actividades_encontradas = []

print("Iniciando búsqueda de tours en Brasil...")

# 3. Procesar cada archivo de texto
for archivo in archivos_sitemap:
    if os.path.exists(archivo):
        print(f"-> Analizando {archivo}...")
        with open(archivo, 'r', encoding='utf-8') as f:
            # Leer línea por línea
            for url in f:
                url = url.strip() # Quitar espacios o saltos de línea
                
                # Revisar si alguna ciudad de Brasil está en esta URL
                for ciudad_slug in ciudades_brasil:
                    if f"/{ciudad_slug}/" in url:
                        # Extraer el ID (todo lo que está entre '-t' y '/')
                        match_id = re.search(r'-t(\d+)/', url)
                        tour_id = match_id.group(1) if match_id else "N/A"
                        
                        # Limpiar un poco el título referencial
                        try:
                            titulo_bruto = url.split(f"/{ciudad_slug}/")[1].split('-t')[0].replace('-', ' ').capitalize()
                        except IndexError:
                            titulo_bruto = "Desconocido"
                        
                        actividades_encontradas.append({
                            "pais": "Brasil",
                            "ciudad_id": ciudad_slug,
                            "tour_id": tour_id,
                            "titulo_referencia": titulo_bruto,
                            "url": url
                        })
    else:
        print(f"⚠️ Archivo no encontrado: {archivo}. Saltando...")

# 4. Exportar los resultados
if actividades_encontradas:
    df = pd.DataFrame(actividades_encontradas)
    # Eliminar posibles duplicados
    df = df.drop_duplicates(subset=['tour_id'])
    
    archivo_salida = 'tours_brasil_IDs.csv'
    df.to_csv(archivo_salida, index=False, sep=';', encoding='utf-8-sig')
    
    print(f"\n✅ ¡ÉXITO ROTUNDO! Se encontraron {len(df)} tours y se guardaron en '{archivo_salida}'")
    print(df.head())
else:
    print("\nNo se encontraron tours. Revisa la lista de ciudades o asegúrate de haber descargado los archivos TXT correctos.")