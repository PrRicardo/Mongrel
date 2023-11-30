# MONGREL - MONgoDb Going RELational

Hi! Thanks for actually reading the README.md!
MONGREL is a tool that allows hierarchical datastructures like MongoDB to be ported to relational datastructures like
PostgreSQL. Currently only these two databases are supported as source and target respectively.

## Practical Example

In our example we need to transfer information about my Spotify playlists into a PostgreSQL database.

### Configuration Files

#### Relations

First we need a configuration file, that mirrors the relations of the entities we are about to create.

```json
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
    // ...
  },
  "music.track_artists": {
    // ...
  },
  "music.alb_artists": {
    // ...
  },
  "music.tracks": {
    // ...
  },
  "music.users": {
    // ...
  }
}
```

Every table defined in the mapping configuration needs to be defined here. Let's take a closer look on the interesting
tables.

```json
{
  "music.album": {
    "track.album.id": "id CHARACTER VARYING (24)",
    "track.album.name": "album_name CHARACTER VARYING(512)",
    "track.album.album_type": "album_type CHARACTER VARYING(32)",
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
  // ...
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
    "track.album.name": "album_name CHARACTER VARYING(512)"
    // ...
  }
  // ...
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
          // Here's the data for alb_artists btw.
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
    // ...
  }
}
```

Let's take a look at the **transfer_options** of the mapping configuration for music.album. The field release_date of
the source document is a string, we want to convert it to a date though. We can do this by adding the field in the
**conversion_fields** and declaring the types.

```json
{
  "music.album": {
    // ...
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
  // ...
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
  // ...
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
      ],
      // ...
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
    ],
    // ...
  },
  // ...
}
```

As we can see there are two relevant sections for artist information. That's why we need two different tables to get all
that information into our relational database and have the relations correctly.
```json
{
  // ...
  "music.track_artists": {
    "track.artists.id": "id CHARACTER VARYING(24)",
    "track.artists.name": "name CHARACTER VARYING(512)",
    "track.artists.type": "type CHARACTER VARYING(128)",
    "transfer_options": {
      "reference_keys": {
        "id": "PK"
      },
      "alias": "music.artists"
    }
  },
  "music.alb_artists": {
    "track.album.artists.id": "id CHARACTER VARYING(24)",
    "track.album.artists.name": "name CHARACTER VARYING(512)",
    "track.album.artists.type": "type CHARACTER VARYING(128)",
    "transfer_options": {
      "reference_keys": {
        "id": "PK"
      },
      "alias": "music.artists"
    }
  } //...
}
```
We define the two tables with their different paths, but we give them the same alias. The alias combines the two tables
into one music.artists table. If there is different information on columns or PKs they get combined.
