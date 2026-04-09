-- Виконайте цей SQL у Supabase SQL Editor (https://supabase.com/dashboard → SQL Editor)

CREATE TABLE IF NOT EXISTS users (
    chat_id TEXT PRIMARY KEY,
    data JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS schedules (
    group_name TEXT PRIMARY KEY,
    data JSONB NOT NULL DEFAULT '{}'::jsonb
);
