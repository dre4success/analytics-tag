CREATE TABLE
    IF NOT EXISTS events (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
        received_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT NOW (),
            user_id VARCHAR(225),
            event_type VARCHAR(255),
            url TEXT
    );