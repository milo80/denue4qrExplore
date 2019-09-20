from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as f

import parameters.Globals as G
import functions.Utilities as U
from functions.FooPlot import create_dict_colors

from functions.FooPySpark import *
import pandas as pd

import requests
import json
import time


class Denue:
    """
        Loads INEGI Denue data to DataFrames
        from downloaded  *.csv
        from INEGI API
    """

    # Class constructor
    def __init__(self, host_):
        #  check all options at :
        # 'https://spark.apache.org/docs/latest/configuration.html'
        self.host = host_
        conf = SparkConf().setAll([('spark.master', self.host),
                                   ('spark.executor.memory', '6g'),
                                   ('spark.app.name', 'denue mining App'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.cores.max', '3'),

                                   ('spark.driver.memory', '4g')])

        self.sc = SparkContext(conf=conf)
        self.sql = SQLContext(self.sc)

    # Load activity class id map
    def load_denue_raw(self, Path_, FileName_) -> object:
        # load csv data
        df = self.sql.read.format("com.databricks.spark.csv") \
            .options(delimiter=',')\
            .options(header='true', inferSchema='true') \
            .load(Path_+FileName_)

        return df

    # Load from CSV file
    def load_denue_csv(self, Path_, FileName_) -> object:
        # Load Postal Codes of CDMX
        FileName = ''
        if '.csv' in str(FileName_):
            FileName = FileName_
        else:
            FileName = str(FileName_)+'.csv'

        df = self.sql.read.format("com.databricks.spark.csv") \
            .options(delimiter=',')\
            .options(header='true', inferSchema='true') \
            .load(Path_+FileName)

        df = df.select(
            f.col('id'),
            f.col('nom_estab'),
            f.col('raz_social'),
            f.col('nombre_act'),
            f.col('codigo_act'),
            f.col('nom_vial'),
            f.col('nom_v_e_1'),
            f.col('nom_v_e_2'),
            f.col('nom_v_e_3'),
            f.col('numero_ext'),
            f.col('tipo_asent'),
            f.col('nomb_asent'),
            f.col('cod_postal'),
            f.col('entidad'),
            f.col('municipio'),
            f.col('localidad'),
            f.col('www'),
            f.col('tipoUniEco'),
            f.col('latitud'),
            f.col('longitud'),
            f.col('fecha_alta')
        )

        return df

    # Maps data to geojson
    def maps_DF_to_geojson(self, df_B):
        print('Mapeando datos a formato GeoJson')
        try:
            geo_model = df_B.rdd.map(
                lambda x: {"type": "Feature",
                           "properties": {
                               "id": U.cleanChars(str(x["id"])),
                               "nom_estab": U.cleanChars(x['nom_estab']),
                               "raz_social": U.cleanChars(x['raz_social']),
                               "nombre_act": U.cleanChars(x['nombre_act']),
                               "categoria": U.cleanChars(x['category']),
                               "color_cat": x['color_cat'],
                               "direccion": [
                                   ' '.join(['Calle:', U.cleanChars(x['nom_vial']), U.cleanChars(x['numero_ext'])]),
                                   'Entre : ' + U.cleanChars(x['nom_v_e_1']) + ' y '+ U.cleanChars(x['nom_v_e_2']),
                                   U.cleanChars(x['tipo_asent']) + ': ' + U.cleanChars(x['nomb_asent']),
                                   'CP: ' + U.cleanChars(x['cod_postal']),
                                   'Municipio: ' + U.cleanChars(x['municipio']),
                                   'Entidad: ' + U.cleanChars(x['entidad'])
                               ],
                               'sitio_web': U.cleanChars(x['www']),
                               'fecha_alta': U.cleanChars(x['fecha_alta']),
                           },
                           "geometry": {
                               "type": "Point",
                               "coordinates": U.verify_geojson_coords(x['longitud'],
                                                                      x['latitud'])
                           }
                           })
            print('data maped to geojson ...')
            geojson = {"type": "FeatureCollection", "features": geo_model.collect()}
        except Exception as e:
            geojson = {"type": "FeatureCollection", "features": []}
            print('Error mapping Economic Units : \n', str(e))

        return geojson

    # Function filter by street name and <Entidad>
    def filter_by_address_and_federal_entity(self, df_B: object, Keywords):
        DataColumns = ['nom_vial', 'entidad']
        df_B = df_B.filter(contains_keyword_and_entity_udf(Keywords)(f.struct(DataColumns))).select('*')


        return df_B

    # Filter by keywords on any valid input field
    def filter_keyword_over_columnField(self, df_B, Keywords, ColField):
        if len(Keywords) > 0:
            df_B = df_B.filter(filter_column_udf(Keywords)(f.struct(ColField)))

        return df_B

    # Match "id_act" VS activity from SCIAN catalog
    def match_id_activity_catalog(self, df_B) -> object:
        # load SCIAN catalog
        df_cat = self.load_denue_raw('./data_sample/',
                                             'denue_ramas.csv')
        df_cat = df_cat.drop_duplicates(subset=['id_act'])
        # Cross <codigo_act> V.C <adtividad>
        a = df_B.alias('a')
        b = df_cat.alias('b')
        df_B = a.join(b, f.col('a.codigo_act') == f.col('b.id_act'), 'left')\
            .select([f.col('a.'+xx) for xx in a.columns] +
                    [f.col('b.actividad').alias('category')])

        return df_B

    # Broups and counts bussiness by category
    # Prints out colors by category
    def group_by_categories_colored(self, df_B, MaxCatDisplay, KeysCategory, CategField):
        CountDict = {}
        df = df_B.groupby(CategField).count().sort(f.col('count').desc())
        #df = self.reduce_category_name(df)
        #
        if len(KeysCategory) > 0:
            df = df.filter(filter_column_udf(KeysCategory)(f.struct([CategField])))
            df_B = df_B.filter(filter_column_udf(KeysCategory)(f.struct([CategField])))

        for item in df.collect():
            CountDict[item[CategField]] = item['count']


        if len(CountDict) >= MaxCatDisplay:
            KeysCategory = list(CountDict.keys())[:MaxCatDisplay]
        else:
            KeysCategory = list(CountDict.keys())

        CountOther = 0
        for Key in CountDict.keys():
            if Key not in KeysCategory:
                CountOther += CountDict[Key]

        CountDict = dict([(i, CountDict[i]) for i in KeysCategory])
        CountDict.update([('Other', CountOther)])

        ColorDict = create_dict_colors(KeysCategory)
        df_B = df_B.withColumn('color_cat',
                               color_category_udf(ColorDict)(f.col(CategField)))


        return df_B, CountDict, ColorDict

    # Reduce Category name with dictionary
    def reduce_category_name(self, df_B):
        ReductDict = {
            "Comercio al por menor de frutas y verduras frescas": "frutasYVerduras",
            "Comercio al por menor de carnes rojas": "carnesRojas",
            "Comercio al por menor en minisupers": "minisupers",
            "Comercio al por menor de carne de aves": "carneAves",
            "Comercio al por menor de dulces y materias primas para repostería": "dulcesYReposteria",
            "Comercio al por menor de cerveza": "cerveza",
            "Comercio al por menor de otros alimentos": "alimentosGral",
            "Comercio al por menor de bebidas no alcohólicas y hielo": "bebidasNoAlcoholicas",
            "Comercio al por menor de leche": "lacteos",
            "Comercio al por menor de artículos de mercería y bonetería": "merceriaYBoneteria",
            "Comercio al por menor de paletas de hielo y helados": "helados",
            "Comercio al por menor de semillas y granos alimenticios": "semillasYGranos",
            "Comercio al por menor de vinos y licores": "vinosYLicores",
            "Comercio al por menor de pescados y mariscos": "pescadosYMariscos",
            "Comercio al por menor en supermercados": "supermercados",
            "Comercio al por menor de blancos": "blancos",
            "Comercio al por menor de telas": "telas",
            "Comercio al por menor en tiendas departamentales": "tiendasDepartamentales",
            "Comercio al por menor de cigarros": "cigarros",
            "Comercio al por menor en tiendas de abarrotes": "abarrotes",
            "Centros de acondicionamiento físico del sector privado": "gimnasioPrivado",
            "Centros de acondicionamiento físico del sector público": "gimnasioPublico",
            "Casas de juegos electrónicos": "juegosElectronicos",
            "Venta de billetes de lotería": "billetesLoteria",
            "Cantantes y grupos musicales del sector privado": "gruposMusicalesPrivado",
            "Grupos musicales del sector público": "gruposMusicalesPublico",
            "Promotores del sector público de espectáculos artísticos": "promotorEspectaculosPublico",
            "Promotores del sector privado de espectáculos artísticos": "promotorEspectaculosPrivado",
            "Promotores de espectáculos artísticos": "promotorEspectaculosArtisticos",
            "Otras compañías y grupos de espectáculos artísticos del sector privado": "espectaculosArtisticosPrivado",
            "Otros servicios recreativos prestados por el sector privado": "serviciosRecreativosPrivado",
            "Otros servicios recreativos prestados por el sector público": "serviciosRecreativosPublico",
            "Parques acuáticos y balnearios del sector privado": "parquesAcuaticosPrivado",
            "Parques acuáticos y balnearios del sector público": "parquesAcuaticosPublico",
            "Museos del sector público": "museosPublico",
            "Museos del sector privado": "museosPrivado",
            "Clubes o ligas de aficionados": "clubesAficionados",
            "Clubes deportivos del sector privado": "clubesDeportivosPrivado",
            "Clubes deportivos del sector público": "clubesDeportivosPublico",
            "Parques de diversiones y temáticos del sector público": "parqueDiversionesPublico",
            "Parques de diversiones y temáticos del sector privado": "parqueDiversionesPrivado",
            "Otros juegos de azar": "juegosAzar",
            "Campos de golf": "camposGolf",
            "Agentes y representantes de artistas": "representantesArtistas",
            "Equipos deportivos profesionales": "equiposDeportivosProfesionales",
            "Billares": "billares",
            "Boliches": "boliches",
            "Artistas": "artistas",
            "Marinas turísticas": "marinasTuristicas",
            "Jardines botánicos y zoológicos del sector privado": "jardinesBotanicosYZologicosPrivado",
            "Jardines botánicos y zoológicos del sector público": "jardinesBotanicosYZologicosPublico",
            "Compañías de teatro del sector privado": "teatroPrivado",
            "Compañías de teatro del sector público": "teatroPublico",
            "Compañías de danza del sector privado": "danzaPrivado",
            "Compañías de danza del sector público": "danzaPublico",
            "Grutas": "grutas",
            "Deportistas profesionales": "deportistasProfesionales",
            "Sitios históricos": "sitiosHistoricos",
            "Restaurantes con servicio de preparación de tacos y tortas": "tacosYtortas",
            "Restaurantes con servicio de preparación de antojitos": "restauranteAntojitos",
            "Cafeterías": "cafeterias",
            "Restaurantes que preparan otro tipo de alimentos para llevar": "restauranteParaLlevar",
            "Restaurantes con servicio de preparación de alimentos a la carta o de comida corrida": "restauranteCarta",
            "Restaurantes con servicio de preparación de pizzas": "pizzas",
            "Servicios de preparación de otros alimentos para consumo inmediato": "comidaRapida",
            "Restaurantes con servicio de preparación de pescados y mariscos": "restauranteMariscos",
            "Bares": "bares",
            "Restaurantes de autoservicio": "restauranteAutoservicio",
            "Hoteles sin otros servicios integrados": "hotelSinOtroServicio",
            "Hoteles con otros servicios integrados": "hotelConOtroServicio",
            "Pensiones y casas de huéspedes": "pensionesHuesped",
            "Servicios de preparación de alimentos para ocasiones especiales": "servicioPreparacionAlimentos",
            "Moteles": "moteles",
            "Centros nocturnos": "centrosNocturnos",
            "Servicios de comedor para empresas e instituciones": "servicioComedorEmpresas",
            "Cabanas": "cabanas",
            "Departamentos y casas amueblados con servicios de hotelería": "hoteleriaServicios",
            "Campamentos y albergues recreativos": "campamentosYAlbergues",
            "Servicios de preparación de alimentos en unidades móviles": "alimentosUnidadesMoviles",
            "Alquiler sin intermediación de salones para fiestas y convenciones": "alquilerFiestasYConvenciones",
            "Alquiler de mesas": "alquilerMesas",
            "Inmobiliarias y corredores de bienes raíces": "corredoresBienesRaices",
            "Servicios de administración de bienes raíces": "administracionBienesRaices",
            "Alquiler sin intermediación de otros bienes raíces": "alquilerBienesRaices",
            "Alquiler de otros artículos para el hogar y personales": "alquilerHogarPersonales",
            "Alquiler de maquinaria y equipo para construcción": "alquilerMaquinariaConstruccion",
            "Alquiler de maquinaria y equipo comercial y de servicios": "alquilerMaquinariaServicios",
            "Alquiler de maquinaria y equipo para mover": "alquilerMaquinariaParaMover",
            "Alquiler de prendas de vestir": "alquilerPrendasDeVestir",
            "Alquiler sin intermediación de viviendas no amuebladas": "alquilerViviendaSinMuebles",
            "Alquiler sin intermediación de viviendas amuebladas": "alquilerViviendaAmueblada",
            "Alquiler de automóviles sin chofer": "alquilerAutomoviles",
            "Alquiler sin intermediación de teatros": "alquilerTeatro",
            "Alquiler sin intermediación de oficinas y locales comerciales": "alquilerOficinas",
            "Otros servicios relacionados con los servicios inmobiliarios": "serviciosInmobiliarios",
            "Centros generales de alquiler": "alquilerEnGeneral",
            "Alquiler de aparatos eléctricos y electrónicos para el hogar y personales": "alquilerElectronicos",
            "Alquiler de equipo de cómputo y de otras máquinas y mobiliario de oficina": "alquilerEqpoComputo",
            "Alquiler de autobuses": "alquilerAutobuses",
            "Alquiler de maquinaria y equipo agropecuario": "alquilerEqpoAgropecuario",
            "Alquiler de camiones de carga sin chofer": "alquilerCamionesDeCarga",
            "Alquiler sin intermediación de edificios industriales dentro de un parque industrial": "alquilerEdifIndustriales",
            "Servicios de alquiler de marcas registradas": "alquilerMarcaRegistrada",
            "Alquiler de equipo de transporte": "alquilerEqpoTransporte",
            "Bufetes jurídicos": "bufetesJuridicos",
            "Servicios de contabilidad y auditoría": "servicioContabilidadYAuditoria",
            "Servicios de fotografía y videograbación": "servicioFotografiaYVideo",
            "Servicios veterinarios para mascotas prestados por el sector privado": "servicioVeterinarioPrivado",
            "Servicios veterinarios para mascotas prestados por el sector público": "servicioVeterinarioPublico",
            "Servicios veterinarios para la ganadería prestados por el sector privado": "servicioVeterinarioGanaderiaPrivado",
            "Servicios veterinarios para la ganadería prestados por el sector público": "servicioVeterinarioGanaderiaPublico",
            "Servicios de consultoría en administración": "servicioConsultoriaAdministracion",
            "Agencias de publicidad": "agenciasPublicidad",
            "Notarías públicas": "notariasPublicas",
            "Servicios de diseno de sistemas de cómputo y servicios relacionados": "servicioSistemasComputo",
            "Servicios de rotulación y otros servicios de publicidad": "servicioPublicidadYRotulacion",
            "Servicios de arquitectura": "servicioArquitectura",
            "Servicios de apoyo para efectuar trámites legales": "servicioTramitesLegales",
            "Servicios de ingeniería": "servicioIngenieria",
            "Diseno gráfico": "disenoGrafico",
            "Otros servicios de consultoría científica y técnica": "servicioConsultoriaCientificaYTec",
            "Laboratorios de pruebas": "laboratorios",
            "Otros servicios profesionales": "servicioProfesionalesGral",
            "Servicios de dibujo": "servicioDibujo",
            "Servicios de consultoría en medio ambiente": "servicioConsultoriaMedioAmbiente",
            "Servicios de investigación de mercados y encuestas de opinión pública": "servicioEncuestas",
            "Diseno y decoración de interiores": "decoracionInteriores",
            "Agencias de anuncios publicitarios": "agenciasAnunciosPublicitarios",
            "Servicios de investigación científica y desarrollo en ciencias naturales y exactas": "investigacionEnCienciasNaturalesYExactas",
            "Servicios de investigación científica y desarrollo en ciencias sociales y humanidades": "investigacionEnCienciasSocialesYHumanidades",
            "Diseno de modas y otros disenos especializados": "disenoModas",
            "Otros servicios relacionados con la contabilidad": "servicioContabilidad",
            "Servicios de elaboración de mapas": "servicioMapas",
            "Servicios de inspección de edificios": "servicioInspeccionEdificios",
            "Agencias de representación de medios": "agenciasMedios",
            "Diseno industrial": "disenoIndustrial",
            "Agencias de relaciones públicas": "agenciaRelacionesPublicas",
            "Servicios de traducción e interpretación": "servicioTraduccion",
            "Distribución de material publicitario": "distribucionMaterialPublicitario",
            "Servicios de levantamiento geofísico": "servicioGeofisica",
            "Agencias de compra de medios a petición del cliente": "agenciaCompras",
            "Agencias de correo directo": "agenciaCorreoDirecto",
            "Escuelas de educación primaria del sector público": "escuelaPrimariaPublico",
            "Escuelas de educación primaria del sector privado": "escuelaPrimariaPrivado",
            "Escuelas de educación preescolar del sector público": "escuelaPreescolarPublico",
            "Escuelas de educación preescolar del sector privado": "escuelaPreescolarPrivado",
            "Escuelas de educación secundaria general del sector público": "escuelaSecundariaPublico",
            "Escuelas de educación secundaria general del sector privado": "escuelaSecundariaPrivado",
            "Escuelas de deporte del sector privado": "escuelaDeportePrivado",
            "Escuelas de deporte del sector público": "escuelaDeportePublico",
            "Escuelas del sector privado que combinan diversos niveles de educación": "escuelaDiversosNivelesPrivado",
            "Escuelas del sector público que combinan diversos niveles de educación": "escuelaDiversosNivelesPublico",
            "Escuelas de educación media superior del sector público": "escuelaMediaSuperiorPublico",
            "Escuelas de educación media superior del sector privado": "escuelaMediaSuperiorPrivado",
            "Escuelas de arte del sector privado": "escuelaArtePrivado",
            "Escuelas de arte del sector público": "escuelaArtePublico",
            "Escuelas de educación superior del sector privado": "escuelaSuperiorPrivado",
            "Escuelas de educación superior del sector público": "escuelaSuperiorPublico",
            "Escuelas del sector privado dedicadas a la ensenanza de oficios": "escuelaDeOficiosPrivado",
            "Escuelas del sector público dedicadas a la ensenanza de oficios": "escuelaDeOficiosPublico",
            "Escuelas de idiomas del sector privado": "escuelaIdiomasPrivado",
            "Escuelas de idiomas del sector público": "escuelaIdiomasPublico",
            "Escuelas de educación secundaria técnica del sector público": "escuelaSecundariaTecnicaPublico",
            "Escuelas de educación secundaria técnica del sector privado": "escuelaSecundatiaTecnicaPrivado",
            "Escuelas de educación técnica superior del sector privado": "escuelaTecnicaSuperiorPrivado",
            "Escuelas de educación técnica superior del sector público": "escuelaTecnicaSuperiorPublico",
            "Servicios de profesores particulares": "profesoresParticulates",
            "Otros servicios educativos proporcionados por el sector privado": "servicioEducativosPrivado",
            "Otros servicios educativos proporcionados por el sector público": "servicioEducativosPublico",
            "Escuelas del sector público de educación para necesidades especiales": "escuelaNecesidadesEspecialesPublico",
            "Escuelas del sector privado de educación para necesidades especiales": "escuelaNecesidadesEspecialesPrivado",
            "Escuelas de computación del sector privado": "escuelaComputacionPrivado",
            "Escuelas de computación del sector público": "escuelaComputacionPublico",
            "Escuelas para la capacitación de ejecutivos del sector privado": "escuelaCapacitacionEjecutivosPrivado",
            "Escuelas para la capacitación de ejecutivos del sector público": "escuelaCapacitacionEjecutivosPublico",
            "Servicios de apoyo a la educación": "servicioApoyoALaEducacion",
            "Escuelas de educación media técnica terminal del sector privado": "escuelaMediaTecnicaPrivado",
            "Escuelas de educación media técnica terminal del sector público": "escuelaMediaTecnicaPublico",
            "Escuelas comerciales y secretariales del sector privado": "escuelaComercialPrivado",
            "Escuelas comerciales y secretariales del sector público": "escuelaComercialPublico",
            "Consultorios dentales del sector privado": "consultorioDentalPrivado",
            "Consultorios de medicina especializada del sector privado": "consultorioMedicinaEspecializadaPrivado",
            "Consultorios de medicina especializada del sector público": "consultorioMedicinaEspecializadaPublico",
            "Consultorios de medicina general del sector privado": "consultorioMedicinaGeneralPrivado",
            "Consultorios de medicina general del sector público": "consultorioMedicinaGeneralPublico",
            "Agrupaciones de autoayuda para alcohólicos y personas con otras adicciones": "grupoAyudaAdicciones",
            "Otros consultorios del sector privado para el cuidado de la salud": "consultorioCuidadoSaludPrivado",
            "Otros consultorios del sector público para el cuidado de la salud": "consultorioCuidadoSaludPublico",
            "Laboratorios médicos y de diagnóstico del sector privado": "laboratorioDiagnosticosPrivado",
            "Laboratorios médicos y de diagnóstico del sector público": "laboratorioDiagnosticosPublico",
            "Guarderías del sector privado": "guarderiasPrivado",
            "Guarderías del sector público": "guarderiaPublico",
            "Consultorios de psicología del sector privado": "consultorioPsicologiaPrivado",
            "Consultorios de psicología del sector público": "consultorioPsicologiaPublico",
            "Consultorios del sector privado de audiología y de terapia ocupacional": "consultorioTerapiaOcupacionalPrivado",
            "Consultorios del sector público de audiología y de terapia ocupacional": "consultorioTerapiaOcupacionalPublico",
            "Consultorios de nutriólogos y dietistas del sector privado": "consultorioNutriologoYDentistaPrivado",
            "Servicios de alimentación comunitarios prestados por el sector público": "servicioComunitarioAlimentacionPublico",
            "Servicios de alimentación comunitarios prestados por el sector privado": "servicioComunitarioAlimentacionPrivado",
            "Servicios de orientación y trabajo social para la niñez y la juventud prestados por el sector público": "servicioTrabajoSocialParaJuventudPublico",
            "Servicios de orientación y trabajo social para la niñez y la juventud prestados por el sector privado": "servicioTrabajoSocialParaJuventudPrivado",
            "Otros servicios de orientación y trabajo social prestados por el sector público": "servicioTrabajoSocialPublico",
            "Otros servicios de orientación y trabajo social prestados por el sector privado": "servicioTrabajoSocialPrivado",
            "Hospitales generales del sector público": "hospitalGeneralPublico",
            "Hospitales generales del sector privado": "hospitalGeneralPrivado",
            "Clínicas de consultorios médicos del sector público": "clinicaMedicosPublico",
            "Clínicas de consultorios médicos del sector privado": "clinicaMedicosPrivado",
            "Consultorios de quiropráctica del sector privado": "consultorioQuiropracticoPrivado",
            "Orfanatos y otras residencias de asistencia social del sector privado": "asistenciaSocialYOrfanatosPrivado",
            "Orfanatos y otras residencias de asistencia social del sector público": "asistenciaSocialYOrfanatosPublico",
            "Hospitales del sector privado de otras especialidades médicas": "hospitalEspecialidadesPrivado",
            "Hospitales del sector público de otras especialidades médicas": "hospitalEspecialidadesPublico",
            "Consultorios de optometría": "consultorioOptometria",
            "Centros del sector público dedicados a la atención y cuidado diurno de ancianos y personas con discapacidad": "centroAtencionAncianosYDiscapacitadosPublico",
            "Centros del sector privado dedicados a la atención y cuidado diurno de ancianos y personas con discapacidad": "centroAtencionAncianosYDiscapacitadosPrivado",
            "Asilos y otras residencias del sector privado para el cuidado de ancianos": "asilosPrivado",
            "Asilos y otras residencias del sector público para el cuidado de ancianos": "asilosPublico",
            "Otros centros del sector privado para la atención de pacientes que no requieren hospitalización": "centroAtencionNoHospitalizadoPrivado",
            "Otros centros del sector público para la atención de pacientes que no requieren hospitalización": "centroAtencionNoHospitalizadoPublico",
            "Consultorios dentales del sector público": "consultorioDentalPublico",
            "Servicios de capacitación para el trabajo prestados por el sector privado para personas desempleadas": "servicioCapacitacionDesempleadosPrivado",
            "Servicios de capacitación para el trabajo prestados por el sector público para personas desempleadas": "servicioCapacitacionDesempleadosPublico",
            "Servicios de enfermería a domicilio": "servicioEnfermeriaDomicilio",
            "Servicios de ambulancias": "servicioAmbulancia",
            "Residencias del sector privado con cuidados de enfermeras para enfermos convalecientes": "residenciaEnfermeriaCuidadosPrivado",
            "Residencias del sector privado para el cuidado de personas con problemas de trastorno mental y adicción": "residenciaTrastornoMentalCuidadosPrivado",
            "Residencias del sector público para el cuidado de personas con problemas de trastorno mental y adicción": "residenciaTrastornoMentalCuidadosPublico",
            "Residencias del sector privado para el cuidado de personas con problemas de retardo mental": "residenciaRetrazoMentalCuidadosPrivado",
            "Centros de planificación familiar del sector privado": "centroPlanificacionFamiliarPrivado",
            "Servicios de bancos de órganos": "servicioBancoDeOrganos",
            "Refugios temporales comunitarios del sector privado": "refugioComunitarioPrivado",
            "Refugios temporales comunitarios del sector público": "refugioComunitarioPublico",
            "Servicios de emergencia comunitarios prestados por el sector privado": "servicioEmergenciaComunitarioPrivado",
            "Servicios de emergencia comunitarios prestados por el sector público": "servicioEmergenciaComunitarioPublico",
            "Hospitales psiquiátricos y para el tratamiento por adicción del sector privado": "hospitalPsiquiatricoAdiccionesPrivado",
            "Hospitales psiquiátricos y para el tratamiento por adicción del sector público": "hospitalPsiquiatricoAdiccionesPublico",
            "Centros del sector privado de atención médica externa para enfermos mentales y adictos": "centroAtencionEnfermosMentalesPrivado",
            "Centros del sector público de atención médica externa para enfermos mentales y adictos": "centroAtencionEnfermosMentalesPublico",
        }

        df_B = df_B.select('*',
                           reduce_name_of_category_udf(ReductDict)(f.struct('category'))
                           .alias('reduct_cat_name'))
        return df_B

    # Alternative to reduce name at the end of excecution
    def reduce_category_name_legend(self, CatDict):
        ReductDict = {"Comercio al por menor": "C_aP_menor",
                  "Comercio al por mayor": "C_aP_mayor",
                  "Restaurantes con servicio de preparación": "RcSP",
                  "Servicios de preparación de otros alimentos para consumo inmediato": "SP_OA_CI",
                  "Restaurantes que preparan otro tipo": "RqP_OT",
                  "Departamentos y casas amueblados": "D_CA"}

        NewDict = {}
        for CatKey in CatDict.keys():
            NewKey = CatKey
            for RKey in ReductDict.keys():
                if str(RKey) in str(CatKey):
                    NewKey = str(CatKey).replace(RKey, ReductDict[RKey])
                    break
            NewDict[NewKey] = CatDict[CatKey]

        return NewDict

    # Maps data model coming from searchExplore extraction
    @staticmethod
    def map_dataframe_to_OCB_model(df: object, Categoria, Tipo) -> list:
        print('Mapeando datos a formato OCB')
        try:
            OCB_model = df.rdd.map(lambda x: {
                "id": "denue-" + str(x["id"]),
                "type": Tipo,
                "address": {
                    "type": "StructuredValue",
                    "value": {
                        "addressCountry": U.cleanChars(x["entidad"]),
                        "addressRegion": U.cleanChars(x["municipio"]),
                        "postalCode": U.cleanChars(x["cod_postal"]),
                        "streetAddress": U.cleanChars(x["nom_vial"]) + "  " +
                                         U.cleanChars(x["numero_ext"]) + ", " +
                                         U.cleanChars(x["tipo_asent"]) + ", " + U.cleanChars(x["nomb_asent"])
                    }
                },
                "category": {
                    "type": "Text",
                    "value": str(Categoria)
                },
                "subCategory": {
                    "type": "Text",
                    "value": U.cleanChars(x["reduct_cat_name"])
                },
                "dateCreated": {
                    "type": "DateTime",
                    "value": U.dateformat_remove_text(x["fecha_alta"]) + "-01T12:00:00.00Z"

                },
                "description": {
                    "type": "Text",
                    "value": U.cleanChars(x["nombre_act"])
                },
                "location": {
                    "type": "geo:json",
                    "value": {
                        "type": "Point",
                        "coordinates": U.verify_geojson_coords(
                            x["longitud"],
                            x["latitud"]
                        )
                    }
                },
                "name": {
                    "type": "Text",
                    "value": U.cleanChars(x["nom_estab"])
                },
                "alternateName": {
                    "type": "Text",
                    "value": U.cleanChars(x["raz_social"])
                },
                "dataProvider": {
                    "type": "Text",
                    "value": x["www"]
                },
                "economyUnit": {
                    "type": "Text",
                    "value": U.cleanChars(x["tipoUniEco"])
                }

            })
            print("mapeo exitoso  ...")
            out = OCB_model.collect()
            print(type(out))
        except Exception as e:
            print('Error en mapeo a OCB : ', str(e))
            out = []

        return out

    # Sends data model to Orion Context Broker
    def send_entities_to_orionCB(self, Data_, URL_):
        k = 0
        for item in Data_:
            try:
                resp_orion = requests.post(URL_,
                                           data=json.dumps(item),
                                           headers=G.HEADERS,
                                           timeout=5)
                print('request[%s] : code = %s ' % (k, resp_orion.status_code))
                if resp_orion.status_code == 201:
                    print('Data sent to ODB : Success! code[%s]' % resp_orion.status_code)
            except requests.exceptions.Timeout as t:
                print('Error', t)
                print('Message : ', resp_orion.text)

            except Exception as e:
                print("Error sending data to Orion Context Broker :\n", str(e))

            # time.sleep(1)
            k += 1

    # Sentds data model to Orion Context Broker , batch mode
    @staticmethod
    def send_entities_to_orionCB_batch(Data) -> None:
        k = 0
        for batch in U.split(G.BATCH_SIZE, Data):
            try:
                resp_orion = requests.post(G.ORION_URL3,
                                           data=json.dumps({"actionType": "APPEND",
                                                            "entities": batch}),
                                           headers=G.HEADERS,
                                           timeout=70)

                #print('type :', type(batch))
                #print('len', len(batch))
                #print(batch[0])
                print('batch request[%s] : code = %s' % (k, resp_orion.status_code))
                if resp_orion.status_code == 201:
                    N = len(batch)
                    print('Data items[%s] sent to OCB : Success! code[%s]' % (N, resp_orion.status_code))
            except requests.exceptions.Timeout as t:
                print('Error,  timeout : ', str(t))

            except Exception as e:
                print("Error sending data to Orion Context Broker :\n", str(e))

            k += 1
