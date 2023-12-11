# MONGREL - MONgodb Going RELational

Hi! Thanks for actually reading the README.md!
MONGREL is a tool that allows hierarchical datastructures like MongoDB to be ported to relational datastructures like
PostgreSQL. Currently only these two databases are supported as source and target respectively.

## Requirements

The required Python version is 3.10+. You'll need a source mongo, a target PostgreSQL and two configuration files.
One configuration file describes the relations of the future target tables (relations.json). The other describes where
the source information for every target table lies within the source document (mappings.json).    
Refer to transfer/configurations/ for examples.

## Features

MONGREL is able to not only able to transfer the data. It understands foreign key relations and respects them during
writing. It is also able to write into an empty database since it creates the required tables automatically.
Here is an example document to follow the documentation with.
```json
{
  "added_by": {
    "id": "ME",
    "type": "user"
  },
  "track": {
    "album": {
      "album_type": "album",
      "artists": [
        {
          "id": "2S5hlvw4CMtMGswFtfdK15",
          "name": "Royal Blood",
          "type": "artist"
        }
      ],
      "id": "0BFzNaeaNv4mahOzwZFGHK",
      "name": "Royal Blood",
      "release_date": "2014-08-22",
      "total_tracks": 10,
      "type": "album"
    },
    "artists": [
      {
        "id": "2S5hlvw4CMtMGswFtfdK15",
        "name": "Royal Blood",
        "type": "artist"
      }
    ],
    "id": "48wDtXGVFNZHANlDYTKMAW",
    "name": "Loose Change",
    "popularity": 60,
    "track_number": 7,
    "type": "track"
  }
}
```


### Defining relations between the source tables

On the relational side, target tables can be in relations with each other. These need to be described manually with
the help of a relation json-file.

```json relations.json
{
  "music.tracks": {
    "_comment": "The table name in the relational Database. A schema is optional",
    "n:1": {
      "_comment": "All entries in this key have a n:1 relationship on the relational database with music.tracks",
      "music.album": {
        "_comment":"Tables in relations can have own relations as well",
        "n:m": {
          "music.alb_artists": {}
        }
      },
      "music.users": {}
    },
    "n:m": {
      "_comment": "music.tracks also has n:m relations, in this case only one with track_artists",
      "music.track_artists": {}
    }
  }
}
```

Every table can have two types of relations.

#### n:1 / 1:n

The common n:1 relation, also used as a 1:1 relation. The n side receives a reference to the 1 side. The reference
contains every field of the reference table's primary key with the prefix (other table name)_(reference field name).
In our relations.json example, music.tracks will have the fields users_id and users_type since the primary key of
music.users is composite of id and type.

#### n:m / m:n

The m:n relation implies the existence of a helper table. If no helper table exists yet, using a n:m relation creates a
helper table automatically, with the primary keys of the two connected tables. Internally a table is created which has
a 1:n relation to both tables. If you want to use your own helper table, you can do so by writing a config like this.

```json relations.json
{
  "schema.left_table": {
    "1:n": {
      "schema.linking_table": {
        "n:1": {
          "schema.right_table": {}
        }
      }
    }
  }
}
```

### Mapping Configuration

```json mappings.json
{
  "music.album": {
    "track.album.id": "id CHARACTER VARYING (24)",
    "track.album.name": "album_name CHARACTER VARYING(511)",
    "track.album.album_type": "album_type CHARACTER VARYING(31)",
    "track.album.release_date": "release_date DATE",
    "track.album.total_tracks": "total_tracks INTEGER",
    "transfer_options": {
      "conversion_fields": {
        "release_date": {
          "source_type": "string",
          "target_type": "date",
          "args": {
            "format": "%Y-%m-%d"
          }
        }
      },
      "reference_keys": {
        "id": "PK"
      }
    }
  }
}
```

#### Defining the source and target fields

With the second configuration file, the mapping of target and source fields need to be described. The configuration
works as follows:

1. The most outer bracket describes the name on the target system. The format is SCHEMA.TABLE. In our example
   music.album.
2. The key of an entry describes the path one must walk through the json to reach the correct value. The value
   seperator is the dot character.    
   Example: If we want to get the id field of the album in our example document, the values we must access are track,
   then album, then id. The key value is therefore track.album.id.
3. The value describes the name and sql definition on the target system. These resemble CREATE-SQL syntax because they
   are used as-is in the creation statement on the target database.

#### Transfer options
The reserved key "transfer_options" is used to store information about the transfer itself.     

##### conversion_fields
With the keyword "conversion_fields", fields can be defined that need to have a conversion function. In our example we 
are converting a string to a date. With the fields "source_type" and "target_type" we define which conversion function 
should be used. The arguments given in "args" are given as keywords arguments to the conversion function. New conversion
functions can be added in transfer/helpers/conversions.py. Don't forget to add your new conversion method in 
get_conversion as well!

##### reference_keys
With the keyword "reference_keys", fields can be defined that have special roles in the transfer. Currently only primary
keys are supported and foreign keys are inferred automatically. In our example we have only one ID-Field as a primary
key, however composites are possible!


## Practical Example

In our example we need to transfer information about my Spotify playlists into a PostgreSQL database.

### Configuration Files

#### Relations

First we need a configuration file, that mirrors the relations of the tables we want to have in our target database
after the transfer is done.

```json relations.json
{
  "music.tracks": {
    "n:1": {
      "music.album": {
        "n:m": {
          "music.alb_artists": {}
        }
      },
      "music.users": {}
    },
    "n:m": {
      "music.track_artists": {}
    }
  }
}
```

We defined a lot of entities here, let's get over every single one of them.     
**music.tracks:**      
The track table has three relations. Two of those are n:1 relations. This means that the finished track table is going
to have an album_id to reference music.album and an users_id which references music.users. Foreign Key constraints will
be created on the database.      
The last relation is an n:m relation to music.track_artists. This implies the creation of a helper table to map this
relation correctly. The helper table will be named music.tracks2track_artists. (In our example it
will be just music.tracks2artists, but we'll get to that later.)

**music.album:**      
This table has two relations. The n:m relation is created like in music.tracks with adjusted naming and references.
However, we need to look at the relation to music.tracks. Since music.tracks has an n:1 to music.album, the relation
is inverted for music.album. This means that music.album has an 1:n relation to music.tracks which does not require any
additional adjustments on the music.album table.

#### Mappings

Next we need to define the mapping from the source document to the target tables. For this we define a second
configuration file.

```json
{
  "music.album": {
  },
  "music.track_artists": {
  },
  "music.alb_artists": {
  },
  "music.tracks": {
  },
  "music.users": {
  }
}
```

Every table defined in the mapping configuration needs to be defined here. Let's take a closer look on the interesting
tables.

```json
{
  "music.album": {
    "track.album.id": "id CHARACTER VARYING (24)",
    "track.album.name": "album_name CHARACTER VARYING(511)",
    "track.album.album_type": "album_type CHARACTER VARYING(31)",
    "track.album.release_date": "release_date DATE",
    "track.album.total_tracks": "total_tracks INTEGER",
    "transfer_options": {
      "conversion_fields": {
        "release_date": {
          "source_type": "string",
          "target_type": "date",
          "args": {
            "format": "%Y-%m-%d"
          }
        }
      },
      "reference_keys": {
        "id": "PK"
      }
    }
  }
}
```

First we define all the fields that we have. The key of a field defines the path one must take to fetch the value in the
source document. Lists / Arrays in the source document do not require special annotation as they are detected and
handled
automatically. The value itself describes the field in the relational datastructure. The structure of the value mirrors
the column definition like in a CREATE-statement.

```json
{
  "music.album": {
    "track.album.id": "id CHARACTER VARYING (24)",
    "track.album.name": "album_name CHARACTER VARYING(511)"
  }
}
```

Here's a cutout of one source document for reference:

```json
{
  "track": {
    "album": {
      "album_type": "album",
      "artists": [
        {
          "_comment": "Here's the data for alb_artists btw."
        }
      ],
      "href": "https://api.spotify.com/v1/albums/1hj1SYbJYdXloRiSjsCLXg",
      "id": "1hj1SYbJYdXloRiSjsCLXg",
      "name": "Raise!",
      "release_date": "1981-11-14",
      "release_date_precision": "day",
      "total_tracks": 9,
      "type": "album",
      "uri": "spotify:album:1hj1SYbJYdXloRiSjsCLXg"
    }
  }
}
```

Let's take a look at the **transfer_options** of the mapping configuration for music.album. The field release_date of
the source document is a string, we want to convert it to a date though. We can do this by adding the field in the
**conversion_fields** and declaring the types.

```json
{
  "music.album": {
    "transfer_options": {
      "conversion_fields": {
        "release_date": {
          "source_type": "string",
          "target_type": "date",
          "args": {
            "format": "%Y-%m-%d"
          }
        }
      },
      "reference_keys": {
        "id": "PK"
      }
    }
  }
}
```

Now that the conversion is declared, every time a value is written to release_date it will get converted through a
conversion function.
Maybe you have already noticed the **reference_keys** section. Here you can define the Primary Key by adding it with the
value 'PK'.

##### Aliasing

Let's continue our configuration with a quick look at aliasing. This is required since there are two sources
for the artists in the source document:

```json
{
  "track": {
    "album": {
      "album_type": "album",
      "artists": [
        {
          "external_urls": {
            "spotify": "https://open.spotify.com/artist/4QQgXkCYTt3BlENzhyNETg"
          },
          "href": "https://api.spotify.com/v1/artists/4QQgXkCYTt3BlENzhyNETg",
          "id": "4QQgXkCYTt3BlENzhyNET  g",
          "name": "Earth, Wind & Fire",
          "type": "artist",
          "uri": "spotify:artist:4QQgXkCYTt3BlENzhyNETg"
        }
      ]
    },
    "artists": [
      {
        "external_urls": {
          "spotify": "https://open.spotify.com/artist/4QQgXkCYTt3BlENzhyNETg"
        },
        "href": "https://api.spotify.com/v1/artists/4QQgXkCYTt3BlENzhyNETg",
        "id": "4QQgXkCYTt3BlENzhyNETg",
        "name": "Earth, Wind & Fire",
        "type": "artist",
        "uri": "spotify:artist:4QQgXkCYTt3BlENzhyNETg"
      }
    ]
  }
}
```

As we can see there are two relevant sections for artist information. That's why we need two different tables to get all
that information into our relational database and have the relations correctly.

```json
{
  "music.track_artists": {
    "track.artists.id": "id CHARACTER VARYING(24)",
    "track.artists.name": "name CHARACTER VARYING(511)",
    "track.artists.type": "type CHARACTER VARYING(127)",
    "transfer_options": {
      "reference_keys": {
        "id": "PK"
      },
      "alias": "music.artists"
    }
  },
  "music.alb_artists": {
    "track.album.artists.id": "id CHARACTER VARYING(24)",
    "track.album.artists.name": "name CHARACTER VARYING(511)",
    "track.album.artists.type": "type CHARACTER VARYING(127)",
    "transfer_options": {
      "reference_keys": {
        "id": "PK"
      },
      "alias": "music.artists"
    }
  }
}
```

We define the two tables with their different paths, but we give them the same alias. The alias combines the two tables
into one music.artists table. If there is different information on columns or PKs they get combined.


### Running

```python
import os
from mongrel import transfer_data_from_mongo_to_postgres

if __name__ == "__main__":
   transfer_data_from_mongo_to_postgres("../configurations/spotify_relations.json",
                                        "../configurations/spotify_mappings.json", mongo_host="localhost",
                                        mongo_database="hierarchical_relational_test", mongo_collection="test_tracks",
                                        sql_host='127.0.0.1', sql_database='ricardo', sql_user='ricardo',
                                        sql_port=5432, sql_password=os.getenv("PASSWORD"))
```