```python
!apt-get update -qq
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz
!tar xf spark-3.1.2-bin-hadoop2.7.tgz
!pip install -q findspark
```

    W: Skipping acquire of configured file 'main/source/Sources' as repository 'https://r2u.stat.illinois.edu/ubuntu jammy InRelease' does not seem to provide it (sources.list entry misspelt?)
    


```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"
```


```python
import findspark
findspark.init()
```


```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Desafio TOTVS") \
    .getOrCreate()
```


```python
spark = SparkSession.builder \
    .appName("Exemplo PySpark") \
    .getOrCreate()
```


```python
file_path = '/content/dados_jogos.csv'
```


```python
dados_jogos = spark.read.csv(file_path, header=True, inferSchema=True, sep=',')
dados_jogos.show()
```

    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    |_c0|  id|               title|           thumbnail|   short_description|            game_url|        genre|    platform|           publisher|           developer|release_date|         profile_url|release_year|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    |  0|1136|         Overwatch 2|https://www.mmobo...|Big changes come ...|https://www.mmobo...|      Shooter|PC (Windows)|Activision Blizza...|Blizzard Entertai...|  2022-10-04|https://www.mmobo...|        2022|
    |  1| 523|            Lost Ark|https://www.mmobo...|Journey throughou...|https://www.mmobo...|         ARPG|PC (Windows)|        Amazon Games|           Smilegate|  2022-02-11|https://www.mmobo...|        2022|
    |  2|1113| PUBG: BATTLEGROUNDS|https://www.mmobo...|Battle the odds i...|https://www.mmobo...|      Shooter|PC (Windows)|       KRAFTON, Inc.|       KRAFTON, Inc.|  2022-01-12|https://www.mmobo...|        2022|
    |  3| 508|            Enlisted|https://www.mmobo...|Step into the mos...|https://www.mmobo...|      Shooter|PC (Windows)|Gaijin Entertainment|   Darkflow Software|  2021-04-08|https://www.mmobo...|        2021|
    |  4|1120|           Fall Guys|https://www.mmobo...|Fall Guys is a fr...|https://www.mmobo...|Battle Royale|PC (Windows)|          Mediatonic|          Mediatonic|  2020-08-04|https://www.mmobo...|        2020|
    |  5| 340|Game Of Thrones W...|https://www.mmobo...|Fame and glory aw...|https://www.mmobo...|     Strategy| Web Browser|            GTArcade|       YOOZOO Games |  2019-11-14|https://www.mmobo...|        2019|
    |  6| 380| Dark Orbit Reloaded|https://www.mmobo...|Take part in huge...|https://www.mmobo...|      Shooter| Web Browser|            Bigpoint|            Bigpoint|  2006-12-11|https://www.mmobo...|        2006|
    |  7|   5|            Crossout|https://www.mmobo...|Trick out your ri...|https://www.mmobo...|      Shooter|PC (Windows)|              Targem|              Gaijin|  2017-05-30|https://www.mmobo...|        2017|
    |  8| 347|             Elvenar|https://www.mmobo...|In InnoGames' Elv...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2015-04-08|https://www.mmobo...|        2015|
    |  9|  11|         Neverwinter|https://www.mmobo...|Neverwinter is an...|https://www.mmobo...|       MMORPG|PC (Windows)|Perfect World Ent...|     Cryptic Studios|  2013-12-06|https://www.mmobo...|        2013|
    | 10| 345|    Forge of Empires|https://www.mmobo...|Forge of Empires ...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2012-04-17|https://www.mmobo...|        2012|
    | 11|   2|      World of Tanks|https://www.mmobo...|World of Tanks is...|https://www.mmobo...|      Shooter|PC (Windows)|           Wargaming|           Wargaming|  2011-04-12|https://www.mmobo...|        2011|
    | 12|1181|           Tarisland|https://www.mmobo...|If you're looking...|https://www.mmobo...|       MMORPG|PC (Windows)|             Tencent|      Level Infinite|  2024-06-21|https://www.mmobo...|        2024|
    | 13|1118|     Diablo Immortal|https://www.mmobo...|Diablo Immortal i...|https://www.mmobo...|      MMOARPG|PC (Windows)| Activision Blizzard|Blizzard Entertai...|  2022-06-02|https://www.mmobo...|        2022|
    | 14|1180|            XDefiant|https://www.mmobo...|Ubisoft enters th...|https://www.mmobo...|      Shooter|PC (Windows)|             Ubisoft|             Ubisoft|  2024-05-21|https://www.mmobo...|        2024|
    | 15|1170|          THE FINALS|https://www.mmobo...|The world is watc...|https://www.mmobo...|      Shooter|PC (Windows)|      Embark Studios|      Embark Studios|  2023-12-07|https://www.mmobo...|        2023|
    | 16|1173|       Titan Revenge|https://www.mmobo...|Become an Angel o...|https://www.mmobo...|       MMORPG| Web Browser|Game Hollywood Games|Game Hollywood Games|  2023-12-20|https://www.mmobo...|        2023|
    | 17|1160|               Palia|https://www.mmobo...|Get your cozy MMO...|https://www.mmobo...|       MMORPG|PC (Windows)|     Singularity Six|     Singularity Six|  2023-08-10|https://www.mmobo...|        2023|
    | 18| 475|      Genshin Impact|https://www.mmobo...|Explore a bright ...|https://www.mmobo...|   Action RPG|PC (Windows)|              miHoYo|              miHoYo|  2020-09-28|https://www.mmobo...|        2020|
    | 19| 458|League of Angels ...|https://www.mmobo...|Embark on an epic...|https://www.mmobo...|       MMORPG| Web Browser|            Gtarcade|        Yoozoo Games|  2020-01-09|https://www.mmobo...|        2020|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    only showing top 20 rows
    
    


```python
dados_jogos_apos2008 = dados_jogos.filter(dados_jogos['release_year'] > 2008)
dados_jogos_apos2008.show()

```

    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    |_c0|  id|               title|           thumbnail|   short_description|            game_url|        genre|    platform|           publisher|           developer|release_date|         profile_url|release_year|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    |  0|1136|         Overwatch 2|https://www.mmobo...|Big changes come ...|https://www.mmobo...|      Shooter|PC (Windows)|Activision Blizza...|Blizzard Entertai...|  2022-10-04|https://www.mmobo...|        2022|
    |  1| 523|            Lost Ark|https://www.mmobo...|Journey throughou...|https://www.mmobo...|         ARPG|PC (Windows)|        Amazon Games|           Smilegate|  2022-02-11|https://www.mmobo...|        2022|
    |  2|1113| PUBG: BATTLEGROUNDS|https://www.mmobo...|Battle the odds i...|https://www.mmobo...|      Shooter|PC (Windows)|       KRAFTON, Inc.|       KRAFTON, Inc.|  2022-01-12|https://www.mmobo...|        2022|
    |  3| 508|            Enlisted|https://www.mmobo...|Step into the mos...|https://www.mmobo...|      Shooter|PC (Windows)|Gaijin Entertainment|   Darkflow Software|  2021-04-08|https://www.mmobo...|        2021|
    |  4|1120|           Fall Guys|https://www.mmobo...|Fall Guys is a fr...|https://www.mmobo...|Battle Royale|PC (Windows)|          Mediatonic|          Mediatonic|  2020-08-04|https://www.mmobo...|        2020|
    |  5| 340|Game Of Thrones W...|https://www.mmobo...|Fame and glory aw...|https://www.mmobo...|     Strategy| Web Browser|            GTArcade|       YOOZOO Games |  2019-11-14|https://www.mmobo...|        2019|
    |  7|   5|            Crossout|https://www.mmobo...|Trick out your ri...|https://www.mmobo...|      Shooter|PC (Windows)|              Targem|              Gaijin|  2017-05-30|https://www.mmobo...|        2017|
    |  8| 347|             Elvenar|https://www.mmobo...|In InnoGames' Elv...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2015-04-08|https://www.mmobo...|        2015|
    |  9|  11|         Neverwinter|https://www.mmobo...|Neverwinter is an...|https://www.mmobo...|       MMORPG|PC (Windows)|Perfect World Ent...|     Cryptic Studios|  2013-12-06|https://www.mmobo...|        2013|
    | 10| 345|    Forge of Empires|https://www.mmobo...|Forge of Empires ...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2012-04-17|https://www.mmobo...|        2012|
    | 11|   2|      World of Tanks|https://www.mmobo...|World of Tanks is...|https://www.mmobo...|      Shooter|PC (Windows)|           Wargaming|           Wargaming|  2011-04-12|https://www.mmobo...|        2011|
    | 12|1181|           Tarisland|https://www.mmobo...|If you're looking...|https://www.mmobo...|       MMORPG|PC (Windows)|             Tencent|      Level Infinite|  2024-06-21|https://www.mmobo...|        2024|
    | 13|1118|     Diablo Immortal|https://www.mmobo...|Diablo Immortal i...|https://www.mmobo...|      MMOARPG|PC (Windows)| Activision Blizzard|Blizzard Entertai...|  2022-06-02|https://www.mmobo...|        2022|
    | 14|1180|            XDefiant|https://www.mmobo...|Ubisoft enters th...|https://www.mmobo...|      Shooter|PC (Windows)|             Ubisoft|             Ubisoft|  2024-05-21|https://www.mmobo...|        2024|
    | 15|1170|          THE FINALS|https://www.mmobo...|The world is watc...|https://www.mmobo...|      Shooter|PC (Windows)|      Embark Studios|      Embark Studios|  2023-12-07|https://www.mmobo...|        2023|
    | 16|1173|       Titan Revenge|https://www.mmobo...|Become an Angel o...|https://www.mmobo...|       MMORPG| Web Browser|Game Hollywood Games|Game Hollywood Games|  2023-12-20|https://www.mmobo...|        2023|
    | 17|1160|               Palia|https://www.mmobo...|Get your cozy MMO...|https://www.mmobo...|       MMORPG|PC (Windows)|     Singularity Six|     Singularity Six|  2023-08-10|https://www.mmobo...|        2023|
    | 18| 475|      Genshin Impact|https://www.mmobo...|Explore a bright ...|https://www.mmobo...|   Action RPG|PC (Windows)|              miHoYo|              miHoYo|  2020-09-28|https://www.mmobo...|        2020|
    | 19| 458|League of Angels ...|https://www.mmobo...|Embark on an epic...|https://www.mmobo...|       MMORPG| Web Browser|            Gtarcade|        Yoozoo Games|  2020-01-09|https://www.mmobo...|        2020|
    | 20|  21|           Destiny 2|https://www.mmobo...|Embark on a heroi...|https://www.mmobo...|      Shooter|PC (Windows)|              Bungie|              Bungie|  2019-10-01|https://www.mmobo...|        2019|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+
    only showing top 20 rows
    
    


```python
from pyspark.sql.functions import when, col
```


```python
dados_jogos = dados_jogos.withColumn('antiguidade',when(col('release_date').substr(1, 4).cast('int') < 2008, 'antigo').otherwise('novo'))
```


```python
dados_jogos.show()
```

    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+-----------+
    |_c0|  id|               title|           thumbnail|   short_description|            game_url|        genre|    platform|           publisher|           developer|release_date|         profile_url|release_year|antiguidade|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+-----------+
    |  0|1136|         Overwatch 2|https://www.mmobo...|Big changes come ...|https://www.mmobo...|      Shooter|PC (Windows)|Activision Blizza...|Blizzard Entertai...|  2022-10-04|https://www.mmobo...|        2022|       novo|
    |  1| 523|            Lost Ark|https://www.mmobo...|Journey throughou...|https://www.mmobo...|         ARPG|PC (Windows)|        Amazon Games|           Smilegate|  2022-02-11|https://www.mmobo...|        2022|       novo|
    |  2|1113| PUBG: BATTLEGROUNDS|https://www.mmobo...|Battle the odds i...|https://www.mmobo...|      Shooter|PC (Windows)|       KRAFTON, Inc.|       KRAFTON, Inc.|  2022-01-12|https://www.mmobo...|        2022|       novo|
    |  3| 508|            Enlisted|https://www.mmobo...|Step into the mos...|https://www.mmobo...|      Shooter|PC (Windows)|Gaijin Entertainment|   Darkflow Software|  2021-04-08|https://www.mmobo...|        2021|       novo|
    |  4|1120|           Fall Guys|https://www.mmobo...|Fall Guys is a fr...|https://www.mmobo...|Battle Royale|PC (Windows)|          Mediatonic|          Mediatonic|  2020-08-04|https://www.mmobo...|        2020|       novo|
    |  5| 340|Game Of Thrones W...|https://www.mmobo...|Fame and glory aw...|https://www.mmobo...|     Strategy| Web Browser|            GTArcade|       YOOZOO Games |  2019-11-14|https://www.mmobo...|        2019|       novo|
    |  6| 380| Dark Orbit Reloaded|https://www.mmobo...|Take part in huge...|https://www.mmobo...|      Shooter| Web Browser|            Bigpoint|            Bigpoint|  2006-12-11|https://www.mmobo...|        2006|     antigo|
    |  7|   5|            Crossout|https://www.mmobo...|Trick out your ri...|https://www.mmobo...|      Shooter|PC (Windows)|              Targem|              Gaijin|  2017-05-30|https://www.mmobo...|        2017|       novo|
    |  8| 347|             Elvenar|https://www.mmobo...|In InnoGames' Elv...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2015-04-08|https://www.mmobo...|        2015|       novo|
    |  9|  11|         Neverwinter|https://www.mmobo...|Neverwinter is an...|https://www.mmobo...|       MMORPG|PC (Windows)|Perfect World Ent...|     Cryptic Studios|  2013-12-06|https://www.mmobo...|        2013|       novo|
    | 10| 345|    Forge of Empires|https://www.mmobo...|Forge of Empires ...|https://www.mmobo...|     Strategy| Web Browser|           InnoGames|           InnoGames|  2012-04-17|https://www.mmobo...|        2012|       novo|
    | 11|   2|      World of Tanks|https://www.mmobo...|World of Tanks is...|https://www.mmobo...|      Shooter|PC (Windows)|           Wargaming|           Wargaming|  2011-04-12|https://www.mmobo...|        2011|       novo|
    | 12|1181|           Tarisland|https://www.mmobo...|If you're looking...|https://www.mmobo...|       MMORPG|PC (Windows)|             Tencent|      Level Infinite|  2024-06-21|https://www.mmobo...|        2024|       novo|
    | 13|1118|     Diablo Immortal|https://www.mmobo...|Diablo Immortal i...|https://www.mmobo...|      MMOARPG|PC (Windows)| Activision Blizzard|Blizzard Entertai...|  2022-06-02|https://www.mmobo...|        2022|       novo|
    | 14|1180|            XDefiant|https://www.mmobo...|Ubisoft enters th...|https://www.mmobo...|      Shooter|PC (Windows)|             Ubisoft|             Ubisoft|  2024-05-21|https://www.mmobo...|        2024|       novo|
    | 15|1170|          THE FINALS|https://www.mmobo...|The world is watc...|https://www.mmobo...|      Shooter|PC (Windows)|      Embark Studios|      Embark Studios|  2023-12-07|https://www.mmobo...|        2023|       novo|
    | 16|1173|       Titan Revenge|https://www.mmobo...|Become an Angel o...|https://www.mmobo...|       MMORPG| Web Browser|Game Hollywood Games|Game Hollywood Games|  2023-12-20|https://www.mmobo...|        2023|       novo|
    | 17|1160|               Palia|https://www.mmobo...|Get your cozy MMO...|https://www.mmobo...|       MMORPG|PC (Windows)|     Singularity Six|     Singularity Six|  2023-08-10|https://www.mmobo...|        2023|       novo|
    | 18| 475|      Genshin Impact|https://www.mmobo...|Explore a bright ...|https://www.mmobo...|   Action RPG|PC (Windows)|              miHoYo|              miHoYo|  2020-09-28|https://www.mmobo...|        2020|       novo|
    | 19| 458|League of Angels ...|https://www.mmobo...|Embark on an epic...|https://www.mmobo...|       MMORPG| Web Browser|            Gtarcade|        Yoozoo Games|  2020-01-09|https://www.mmobo...|        2020|       novo|
    +---+----+--------------------+--------------------+--------------------+--------------------+-------------+------------+--------------------+--------------------+------------+--------------------+------------+-----------+
    only showing top 20 rows
    
    


```python
from pyspark.sql.functions import col, count
```


```python
genre_count = dados_jogos.groupBy('genre').agg(count('genre').alias('count'))
```


```python
genre_count = genre_count.orderBy(col('count').desc())
genre_count.show(1)
```

    +------+-----+
    | genre|count|
    +------+-----+
    |MMORPG|  153|
    +------+-----+
    only showing top 1 row
    
    


```python
dados_jogos_NCSOFT = dados_jogos.filter(dados_jogos['publisher'] == 'NCSOFT')
dados_jogos_NCSOFT.show()
```

    +---+---+-----+--------------------+--------------------+--------------------+------+------------+---------+---------+------------+--------------------+------------+-----------+
    |_c0| id|title|           thumbnail|   short_description|            game_url| genre|    platform|publisher|developer|release_date|         profile_url|release_year|antiguidade|
    +---+---+-----+--------------------+--------------------+--------------------+------+------------+---------+---------+------------+--------------------+------------+-----------+
    |353|254| AION|https://www.mmobo...|Aion: Ascension i...|https://www.mmobo...|MMORPG|PC (Windows)|   NCSOFT|   NCSOFT|  2008-11-25|https://www.mmobo...|        2008|       novo|
    +---+---+-----+--------------------+--------------------+--------------------+------+------------+---------+---------+------------+--------------------+------------+-----------+
    
    


```python
dados_jogos_antigos = dados_jogos.orderBy(col('release_date').asc())
dados_jogos_antigos.show(4)
```

    +--------------+-------------+----------------+--------------------+--------------------+--------------------+--------+------------+---------+---------+------------+--------------------+------------+-----------+
    |           _c0|           id|           title|           thumbnail|   short_description|            game_url|   genre|    platform|publisher|developer|release_date|         profile_url|release_year|antiguidade|
    +--------------+-------------+----------------+--------------------+--------------------+--------------------+--------+------------+---------+---------+------------+--------------------+------------+-----------+
    |           404|          405|Pocket Starships|https://www.mmobo...|Blast off into ad...|https://www.mmobo...|Strategy| Web Browser|     Spyr|     Spyr|        null|https://www.mmobo...|        null|       novo|
    |           250|          169|      Dirty Bomb|https://www.mmobo...|Dirty Bomb, forme...|https://www.mmobo...| Shooter|PC (Windows)|        "|     null|        null|                null|        null|       novo|
    |Warchest Ltd."|Splash Damage|      2015-06-01|https://www.mmobo...|                2015|                null|    null|        null|     null|     null|        null|                null|        null|       novo|
    |           403|          339|           Tibia|https://www.mmobo...|Tibia is a classi...|https://www.mmobo...|  MMORPG|PC (Windows)|  CipSoft|  CipSoft|  1997-01-07|https://www.mmobo...|        1997|     antigo|
    +--------------+-------------+----------------+--------------------+--------------------+--------------------+--------+------------+---------+---------+------------+--------------------+------------+-----------+
    only showing top 4 rows
    
    


```python
titles_per_platform = dados_jogos.groupBy('platform').agg(count('title').alias('num_titles'))
titles_per_platform = titles_per_platform.orderBy(col('num_titles').desc())
```


```python
titles_per_platform.show()
```

    +--------------------+----------+
    |            platform|num_titles|
    +--------------------+----------+
    |        PC (Windows)|       313|
    |         Web Browser|        72|
    |PC (Windows), Web...|        11|
    |              MMORPG|         3|
    |                null|         1|
    |             Shooter|         1|
    |   or more difficult|         1|
    | you'll build a d...|         1|
    |            Strategy|         1|
    | and you'll need ...|         1|
    |https://www.mmobo...|         1|
    +--------------------+----------+
    
    


```python

```
