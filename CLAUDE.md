# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bussig is a PostgreSQL-based message bus library for .NET. It provides distributed messaging with queues, dead letter queues, delayed message processing, distributed locks, message scheduling, and large message attachments via the claim check pattern.

## Build & Development Commands

```bash
# Build solution
dotnet build

# Run all tests
dotnet test

# Run specific test project
dotnet run --project src/Bussig.Tests.Unit
dotnet run --project src/Bussig.Tests.Integration

# Run a single test (TUnit uses --filter)
dotnet test --filter "FullyQualifiedName~MessageUrnTests"

# Format code (CSharpier)
dotnet tool restore
dotnet csharpier format .

# Check formatting without modifying
dotnet csharpier check .
```

**Note:** Integration tests require Docker for TestContainers (PostgreSQL).

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Application Layer                     │
│         Uses IBus.SendAsync / ScheduleAsync              │
├──────────────────────────────────────────────────────────┤
│                   Bussig.Abstractions                    │
│  IBus, IProcessor<T>, IMessage, ICommand, IEvent         │
│  IOutgoingMessageSender, IDistributedLockManager         │
├──────────────────────────────────────────────────────────┤
│                        Bussig                            │
│  Bus, PostgresMigrator, PostgresOutgoingMessageSender    │
│  SystemTextJsonMessageSerializer, DI configuration       │
├──────────────────────────────────────────────────────────┤
│              Bussig.EntityFrameworkCore                   │
│  EF Core outbox pattern (provider-agnostic)              │
├──────────────────────────────────────────────────────────┤
│                Bussig.Azure.Storage                       │
│  Azure Blob Storage attachment repository                │
├──────────────────────────────────────────────────────────┤
│                      PostgreSQL                          │
│  PL/pgSQL functions (get_messages, send_message, etc.)   │
│  Tables: queues, messages, message_delivery, locks       │
└──────────────────────────────────────────────────────────┘
```

**Key Design Decisions:**
- All database operations use PL/pgSQL functions defined in `src/Bussig/infrastructure.sql`
- Supports ambient transactions via `IPostgresTransactionAccessor`
- Message URN generation uses `[MessageMapping]` attribute or type name convention
- Two processor interfaces: `IProcessor<TMessage>` (fire-and-forget) and `IProcessor<TMessage, TSendMessage>` (with reply)
- Attachments use `MessageData` with lazy loading (`GetValueAsync`) and an optional inline threshold for small payloads
- Outbox uses the decorator pattern on `IOutgoingMessageSender` via EF Core

## Project Structure

- `src/Bussig.Abstractions/` - Public interfaces and contracts (IBus, IProcessor, message types)
- `src/Bussig/` - Core PostgreSQL implementation (Bus, migrations, serialization, middleware, DI)
- `src/Bussig.EntityFrameworkCore/` - EF Core outbox pattern (provider-agnostic, depends on `Microsoft.EntityFrameworkCore.Relational`)
- `src/Bussig.Azure.Storage/` - Azure Blob Storage attachment repository (claim check pattern)
- `src/Bussig.Tests.Unit/` - Unit tests (TUnit framework)
- `src/Bussig.Tests.Integration/` - Integration tests with TestContainers
- `src/Bussig.EntityFrameworkCore.Tests/` - EF Core outbox tests
- `src/Bussig.Azure.Storage.Tests.Integration/` - Azure Storage integration tests
- `src/examples/Playground/` - Sample app for manual testing
- `src/examples/EFCore/` - Sample app demonstrating EF Core outbox usage

## Code Standards

- C# 14 / .NET 10.0
- Nullable reference types enabled
- Warnings treated as errors
- CSharpier for formatting (run before committing)
- TUnit for testing with Moq for mocking
