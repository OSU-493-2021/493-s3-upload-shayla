require 'aws-sdk'
require 'aws-sdk-dynamodb'
require 'highline/import'

#whoops forgot to reference sources: 
#https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/hello.html
#https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadObjSingleOpRuby.html

NO_SUCH_BUCKET = "The bucket '%s' does not exist!"

USAGE = <<DOC

Usage: ruby app.rb [operation] [first_name]

Where:
    bucket_name (required) is the name of the bucket

    operation   is the operation to perform on the bucket:
    create  - creates a new bucket
    upload  - uploads a file to the bucket
    list    - (default) lists up to 50 bucket items

    first_name   is the name of the file to upload,
    required when operation is 'upload'

DOC

#assume the role
#arn:aws:iam::297181656029:role/S3-accessor
role_credentials = Aws::AssumeRoleCredentials.new(
    client: Aws::STS::Client.new,
    role_arn: "arn:aws:iam::297181656029:role/S3-accessor",
    role_session_name: "s3-upload-session"
)

#create S3 resource
s3 = Aws::S3::Resource.new(credentials: role_credentials)

# Set name of bucket on which operations are performed
#REQUIRED
bucket_name = nil

if ARGV.length > 0
    bucket_name = ARGV[0]
else
    puts USAGE
    exit 1
end

# The operation to perform on the bucket
operation = 'list' # default
operation = ARGV[1] if (ARGV.length > 1)

# The file name to use with 'upload'
file = nil
file = ARGV[2] if (ARGV.length > 2)

#set second name for rename function
second_name = nil
second_name = ARGV[3] if (ARGV.length > 3)

# Get the bucket by name
bucket = s3.bucket(bucket_name)

def insert(dynamodb_client, table_item)
    dynamodb_client.put_item(table_item)
    puts "Added song '#{table_item[:item][:genre]} " \
        "(#{table_item[:item][:artist]})'."
    rescue StandardError => e
    puts "Error adding song '#{table_item[:item][:genre]} " \
        "(#{table_item[:item][:artist]})': #{e.message}"
end

def setup(genre, artist, album, song, key)
    region = 'us-east-1'
    table_name = 'music'

    Aws.config.update(
        region: region
    )

    dynamodb_client = Aws::DynamoDB::Client.new

    items = []

    item = {

        pk: "song",
        sk: "song##{song}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "artist##{artist}",
        sk: "song##{song}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "album##{album}",
        sk: "song##{song}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "genre##{genre}",
        sk: "song##{song}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "album",
        sk: "album##{album}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "artist##{artist}",
        sk: "album##{album}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "genre##{genre}",
        sk: "album##{album}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "artist",
        sk: "artist##{artist}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    item = {
        pk: "genre##{genre}",
        sk: "artist##{artist}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    # item = {
    #     pk: "genre##{genre}",
    #     sk: "artist##{artist}",
    #     info: {
    #         genre: genre,
    #         artist: artist,
    #         album: album,
    #         song: song,
    #         key: key
    #     }
    # }
    # items.push(item)

    item = {
        pk: "genre",
        sk: "genre##{genre}",
        info: {
            genre: genre,
            artist: artist,
            album: album,
            song: song,
            key: key
        }
    }
    items.push(item)

    table_item = {
        table_name: table_name,
        item: item
    }

    items.each do |item|
        table_item = {
            table_name: table_name,
            item: item
        }
    
            puts "Adding song to table '#{table_name}'..."
            insert(dynamodb_client, table_item)
    end
end

def set_vals(artist: nil, album: nil, song:, key:)
    genre = ask "Input genre: "
    if artist == nil
        artist = ask "Input artist: "
    end
    if album == nil
        album = ask "Input album: "
    end
    setup(genre, artist, album, song, key)
end

#assess what action to take given various
#command line arguments
case operation
    when 'add_bucket'
    # Create a bucket if it doesn't already exist
        if bucket.exists?
            puts "The bucket '%s' already exists!" % bucket_name
        else
            bucket.create
            puts "Created new S3 bucket: %s" % bucket_name
        end

    when 'add_song'
        if file == nil
            puts "Please enter the song title you wish to upload"
            exit
        end

        if bucket.exists?
            name = File.basename file

        # Check if file is already in the bucket
        if bucket.object(name).exists?
            puts "#{name} already exists in the bucket"
        else
            puts "Calling set vals in ADD_SONG"
            puts name
            set_vals(song: name, key: file)
            obj = s3.bucket(bucket_name).object(name)
            obj.upload_file(file)
            puts "Uploaded '%s' to S3!" % name
        end
        else
            NO_SUCH_BUCKET % bucket_name
        end

    when 'list'
        if bucket.exists?
            # Enumerate the bucket contents and object etags
            puts "Contents of '%s':" % bucket_name
            puts '  Name => GUID'

            bucket.objects.limit(50).each do |obj|
            puts "  #{obj.key} => #{obj.etag}"
            end
        else
            NO_SUCH_BUCKET % bucket_name
        end

    when 'add_artist'
        if file == nil
            puts "Please enter the path to the artist directory"
            exit
        else
            artist = File.basename(file, '.*')
            path = Pathname(artist).each_child {|song|
                if song.directory?
                    Dir.each_child(song) do |title|
                        s3.client.put_object( bucket: bucket_name, key: "#{song}/#{title}")
                    end
                end
            }
            puts "#{artist} has been added to #{bucket_name}!!!"
        end

    when 'add_album'
        if file == nil
            puts "Please enter the path to the album directory"
            exit
        else
            album = File.basename(file, '.*')
            Dir.each_child(file) do |song|
                    s3.client.put_object( bucket: bucket_name, key: "#{album}/#{song}")
                    file_path = file + "/" + song
                    puts "Bout to SET_VALS in ADD_ALBUM"
                    puts "album: #{file}, song: #{song}, key: #{file_path}"
                    set_vals(album: file, song: song, key: file_path)
                end
            puts "#{album} has been added to #{bucket_name}!!!"
        end

    when 'rename'
        if file == nil && second_name == nil
            puts "Please enter the original filename and the new file name"
            exit
        else
            first_name=File.basename file 
            s3.client.copy_object(
                bucket: bucket_name,
                copy_source: "#{bucket_name}/#{first_name}",
                key: second_name)
    
            s3.client.delete_object(
                bucket: bucket_name,
                key: first_name)
            puts "#{first_name} has been renamed #{second_name}."
        end

else
    puts "Unknown operation: '%s'!" % operation
    puts USAGE
end


