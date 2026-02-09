using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace EFCore.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "OutboxMessage",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    MessageId = table.Column<Guid>(type: "uuid", nullable: false),
                    QueueName = table.Column<string>(type: "text", nullable: false),
                    Body = table.Column<byte[]>(type: "bytea", nullable: false),
                    HeadersJson = table.Column<string>(type: "text", nullable: true),
                    Priority = table.Column<short>(type: "smallint", nullable: true),
                    Delay = table.Column<TimeSpan>(type: "interval", nullable: true),
                    MessageVersion = table.Column<int>(type: "integer", nullable: false, defaultValue: 1),
                    ExpirationTime = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    SchedulingTokenId = table.Column<Guid>(type: "uuid", nullable: true),
                    CreatedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    PublishedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OutboxMessage", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "SomeEntities",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Name = table.Column<string>(type: "text", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SomeEntities", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessage_Id",
                table: "OutboxMessage",
                column: "Id");

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessage_PublishedAt",
                table: "OutboxMessage",
                column: "PublishedAt");

            migrationBuilder.CreateIndex(
                name: "IX_OutboxMessage_SchedulingTokenId",
                table: "OutboxMessage",
                column: "SchedulingTokenId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "OutboxMessage");

            migrationBuilder.DropTable(
                name: "SomeEntities");
        }
    }
}
