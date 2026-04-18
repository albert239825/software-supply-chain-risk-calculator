import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.SUPABASE_DB_URL || '',
  ssl: { rejectUnauthorized: false },
});

export default pool;
