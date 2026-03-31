# Snowflake MCP server setup guide for Claude

Internal documentation for configuring a Snowflake MCP server to work with Claude.ai via Cortex Agent, Cortex Analyst, and SQL execution.

---

## Overview

Snowflake's MCP (Model Context Protocol) server allows Claude.ai to interact directly with your Snowflake environment. Once configured, users can query data, run SQL, and interact with Cortex Analyst and Cortex Agent — all from the Claude chat interface.

### Supported tool types

Snowflake currently supports 5 tool types on the MCP server:

| Tool type | Purpose |
|-----------|---------|
| `CORTEX_ANALYST_MESSAGE` | Cortex Analyst — natural language to SQL via semantic models |
| `CORTEX_AGENT_RUN` | Cortex Agent — orchestrates across multiple tools |
| `SYSTEM_EXECUTE_SQL` | Direct SQL execution against the connected database. Note that you most likely would not need this tool as it gives users access to query any table in the database. |
| `CORTEX_SEARCH_SERVICE_QUERY` | Cortex Search Service for unstructured data retrieval |
| `GENERIC` | Custom UDFs and stored procedures |

---

## Setup steps

### Step 1: Create the MCP server

Define the MCP server with the tools you want to expose. Customize the `tools` list based on your use case.

```sql
CREATE OR REPLACE MCP SERVER <database>.<schema>.<mcp_server_name>
  FROM SPECIFICATION $$
    tools:
      - title: "Performance Marketing Analyst"
        name: "performance-marketing-analyst"
        type: "CORTEX_ANALYST_MESSAGE"
        identifier: "<database>.<schema>.<semantic_view_name>"
        description: "Cortex Analyst that handles queries around performance marketing e.g. ROAS, CAC, spend, revenue, etc."

      - title: "Cortex Agent"
        name: "cortex-Agent"
        type: "CORTEX_AGENT_RUN"
        identifier: "<database>.<schema>.<agent_name>"
        description: "Cortex Agent that orchestrates across analyst and search tools for marketing analytics."
  $$;
```

**Notes:**
- The `name` field is what appears in the Claude UI when the tool is invoked.
- The `identifier` for `CORTEX_ANALYST_MESSAGE` is the fully qualified semantic view name.
- The `identifier` for `CORTEX_AGENT_RUN` is the fully qualified agent name. Use `SHOW AGENTS` to find the correct identifier.
- The semantic view will be visible in Snowflake's database explorer after creation.
- You can add as many tools to the server with multiple analyst. 

---

### Step 2: Create the OAuth security integration

This allows Claude.ai to authenticate with your Snowflake account via OAuth.

```sql
CREATE OR REPLACE SECURITY INTEGRATION <integration_name>
  TYPE = OAUTH
  OAUTH_CLIENT = CUSTOM
  ENABLED = TRUE
  OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
  OAUTH_REDIRECT_URI = 'https://claude.ai/api/mcp/auth_callback';
```

The redirect URI must be exactly `https://claude.ai/api/mcp/auth_callback` — this is Claude.ai's OAuth callback endpoint.

---

### Step 3: Retrieve OAuth client credentials

Run the following to get the client ID and client secret needed for the Claude UI:

```sql
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('<integration_name>');
```

This returns a JSON object like:

```json
{
  "OAUTH_CLIENT_SECRET_2": "xxxxxxxxxxxx",
  "OAUTH_CLIENT_SECRET": "xxxxxxxxxxxx",
  "OAUTH_CLIENT_ID": "xxxxxxxxxxxx"
}
```

Save the `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET` — these will be shared with users connecting from Claude.

---

### Step 4: Construct the MCP server URL

The server URL follows this pattern:

```
https://<account_identifier>.snowflakecomputing.com/api/v2/databases/<database>/schemas/<schema>/mcp-servers/<mcp_server_name>
```

---

### Step 5: Grant MCP server usage to a role

Grant `USAGE` on the MCP server to the role that will be used by connecting users. This role should also have access to the underlying database, schema, warehouse, and any Cortex services.

```sql
-- Grant MCP server access to the role or any role that users who are using it are on. 
GRANT USAGE ON MCP SERVER <database>.<schema>.<mcp_server_name>
  TO ROLE <role_name>;

-- Ensure the role also has access to underlying objects
GRANT USAGE ON DATABASE <database> TO ROLE <role_name>;
GRANT USAGE ON SCHEMA <database>.<schema> TO ROLE <role_name>;
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;
```

---

### Step 6: Set the user's default role

Each user connecting via Claude should have their default role set to the role with MCP access.
This applies when you are just creating a user for the sake of connecting to the MCP Server. If a user already exists with another role, grant the users role usage to the mcp server in step 5

```sql
ALTER USER <username> SET DEFAULT_ROLE = '<role_name>';
```

Alternatively, grant the role to the user if it's not already assigned:

```sql
GRANT ROLE <role_name> TO USER <username>;
```

---

### Step 7: Whitelist Claude's outbound IP (if applicable)

If your Snowflake instance has a network policy with IP whitelisting, add Claude's outbound IP range:

```
160.79.104.0/21
```

```sql
-- Example: add to an existing network policy
ALTER NETWORK POLICY <policy_name> SET
  ALLOWED_IP_LIST = ('existing_ip_1', '160.79.104.0/21');
```

---

### Step 8: Show delegated authorization to Server

See users that have access to the MCP Server

```sql
SHOW DELEGATED AUTHORIZATIONS TO SECURITY INTEGRATION CLAUDE_MCP_OAUTH;
```

---

### Step 9: Share connection details with users

Provide each user with the following to connect from Claude.ai:

For clients with Claude Team access, the Claude Admin will just use this detail to create the snowflake connection and other users will just connect and authenticate with snowflake. 

1. **MCP server URL** (from step 4)
2. **OAuth client ID** (from step 3)
3. **OAuth client secret** (from step 3)

Users enter these in Claude.ai under **Settings → Integrations → Snowflake** (or by adding a custom MCP server).

---

## User onboarding checklist

For each new user that needs access:

- [ ] Grant the MCP role to the user: `GRANT ROLE <role_name> TO USER <username>;`
- [ ] Set default role: `ALTER USER <username> SET DEFAULT_ROLE = '<role_name>';` (Not compulsory)
- [ ] Grant user default role usage on MCP Server. `GRANT USAGE ON MCP SERVER ANALYTICS.REPORTING.SNOWFLAKE_AGENT_MCP_SERVER
   TO ROLE CORTEX_AGENT_USER_ROLE;`

- [ ] Share server URL, client ID, and client secret with the user

---

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| OAuth authentication failure | Verify delegated authorization is added for the user |
| MCP server not found | Check the server URL matches the exact pattern with correct database, schema, and server name |
| Permission denied on queries | Ensure the user's default role has `USAGE` on the MCP server and underlying objects |
| Connection timeout | Verify Claude's IP range (`160.79.104.0/21`) is whitelisted in your network policy |
| Tools not appearing in Claude | Check the MCP server specification — `name` field controls what appears in the UI |

---

## Reference SQL

### Verify MCP server grants

```sql
SHOW GRANTS ON MCP SERVER <database>.<schema>.<mcp_server_name>;
```

### List available agents

```sql
SHOW AGENTS;
```

### Check user's current roles

```sql
SHOW GRANTS TO USER <username>;
```

### View MCP server definition

```sql
SHOW MCP SERVERS IN SCHEMA <database>.<schema>;
```

DESCRIBE SECURITY INTEGRATION CLAUDE_MCP_OAUTH;
SHOW DELEGATED AUTHORIZATIONS TO SECURITY INTEGRATION CLAUDE_MCP_OAUTH;