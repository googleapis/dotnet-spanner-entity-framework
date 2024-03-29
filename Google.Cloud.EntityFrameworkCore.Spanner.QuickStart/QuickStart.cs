using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

public class Singer
{
    public Singer()
    {
        Albums = new HashSet<Album>();
    }

    public Guid SingerId { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    // FullName is generated automatically from FirstName and LastName.
    public string FullName { get; set; }

    public virtual ICollection<Album> Albums { get; set; }
}

public class Album
{
    public Album()
    {
        Tracks = new HashSet<Track>();
    }

    public Guid AlbumId { get; set; }
    public string Title { get; set; }
    public virtual Singer Singer { get; set; }

    public virtual ICollection<Track> Tracks { get; set; }
}

public class Track
{
    public Track()
    {
    }

    public Guid AlbumId { get; set; }
    public long TrackId { get; set; }
    public string Title { get; set; }

    public virtual Album Album { get; set; }
}

public class MusicDbContext : DbContext
{
    private readonly string _connectionString;

    public MusicDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    public MusicDbContext(string connectionString, DbContextOptions<MusicDbContext> options)
        : base(options)
    {
        _connectionString = connectionString;
    }

    public virtual DbSet<Singer> Singers { get; set; }
    public virtual DbSet<Album> Albums { get; set; }
    public virtual DbSet<Track> Tracks { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        // Configure Entity Framework to use a Cloud Spanner database.
        => options.UseSpanner(_connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Album>(entity =>
        {
            entity.HasKey(entity => new { entity.AlbumId });
        });

        modelBuilder.Entity<Singer>(entity =>
        {
            // FullName is generated by Cloud Spanner on each add or update.
            entity.Property(e => e.FullName).ValueGeneratedOnAddOrUpdate();
        });

        modelBuilder.Entity<Track>(entity =>
        {
            // Track is INTERLEAVED IN PARENT Album.
            entity
                .InterleaveInParent(typeof(Album), OnDelete.Cascade)
                .HasKey(entity => new { entity.AlbumId, entity.TrackId });
        });
    }
}

public static class QuickStart
{
    static void Main(string[] args)
    {
        // TODO: Replace "my-project", "my-instance" and "my-database", with an
        // actual project, instance and database.
        string connectionString = "Data Source=projects/my-project/instances/my-instance/databases/my-database";
        // Create a DbContext that uses our sample Spanner database.
        using var context = new MusicDbContext(connectionString);
        InsertData(context).WaitWithUnwrappedExceptions();
        QueryData(context).WaitWithUnwrappedExceptions();
        QueryDataWithLinq(context).WaitWithUnwrappedExceptions();
    }

    private static async Task InsertData(MusicDbContext context)
    {
        var singer = new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Bob",
            LastName = "Allison",
        };
        context.Singers.Add(singer);
        var album = new Album
        {
            AlbumId = Guid.NewGuid(),
            Title = "Let's Go",
            Singer = singer,
        };
        context.Albums.Add(album);
        var track = new Track
        {
            Album = album,
            TrackId = 1L,
            Title = "Go, Go, Go",
        };
        context.Tracks.Add(track);

        // This saves all the above changes in one transaction.
        Console.WriteLine("Writing Singer, Album and Track to the database...");
        var count = await context.SaveChangesAsync();
        Console.WriteLine($"{count} records written to the database\n");
    }

    private static async Task QueryData(MusicDbContext context)
    {
        Console.WriteLine("Querying singers...");
        var singers = await context.Singers
            .Where(s => s.FullName == "Bob Allison")
            .ToListAsync();
        Console.WriteLine($"Found {singers.Count} singer(s) with full name {singers.First().LastName}");
    }

    private static async Task QueryDataWithLinq(MusicDbContext context)
    {
        Console.WriteLine("Querying singers with LINQ...");
        var singersStartingWithBo = context.Singers
            .Where(s => s.FullName.StartsWith("Bo"))
            .OrderBy(s => s.LastName)
            .AsAsyncEnumerable();
        Console.WriteLine("Singers with a name starting with 'Bo':");
        await foreach (var singer in singersStartingWithBo)
        {
            Console.WriteLine($"{singer.FullName}");
        }
    }
}
