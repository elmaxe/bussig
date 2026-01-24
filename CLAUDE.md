# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bussig is a PostgreSQL-based message bus library for .NET. It provides distributed messaging with queues, dead letter queues, delayed message processing, distributed locks, and message scheduling.

## Build & Development Commands

```bash
# Build solution
dotnet build

# Run all tests
dotnet test

# Run specific test project
dotnet test src/Bussig.Tests.Unit
dotnet test src/Bussig.Tests.Integration

# Run a single test (TUnit uses --filter)
dotnet test --filter "FullyQualifiedName~MessageUrnTests"

# Format code (CSharpier)
dotnet tool restore
dotnet csharpier .

# Check formatting without modifying
dotnet csharpier --check .
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

## Project Structure

- `src/Bussig.Abstractions/` - Public interfaces and contracts (IBus, IProcessor, message types)
- `src/Bussig/` - PostgreSQL implementation (Bus, migrations, serialization, DI extensions)
- `src/Bussig.Tests.Unit/` - Unit tests (TUnit framework)
- `src/Bussig.Tests.Integration/` - Integration tests with TestContainers

## Code Standards

- C# 14 / .NET 10.0
- Nullable reference types enabled
- Warnings treated as errors
- CSharpier for formatting (run before committing)
- TUnit for testing with Moq for mocking
