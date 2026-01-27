## What is an ORM?

An **Object-Relational Mapper** bridges the gap between object-oriented code and relational databases. It lets you work with database records as if they were regular objects in your programming language, without writing raw SQL.

## Core Features

**1. Model Definition**
- Define database tables as classes/structs in your language
- Specify field types, constraints, and relationships
- The ORM handles creating the actual database schema

**2. CRUD Operations**
- **Create**: Insert new records
- **Read**: Query and retrieve records
- **Update**: Modify existing records  
- **Delete**: Remove records

All without writing SQL directly.

**3. Query Interface**
- Build queries using method chaining or a DSL
- Examples: `User.where(age > 18).order_by('name').limit(10)`
- The ORM translates this to SQL behind the scenes

**4. Relationships**
- **One-to-many**: A user has many posts
- **Many-to-one**: Many posts belong to one user
- **Many-to-many**: Users have many roles, roles have many users
- Automatic handling of foreign keys and join tables

**5. Lazy/Eager Loading**
- **Lazy**: Load related data only when accessed
- **Eager**: Load everything upfront to avoid N+1 query problems

**6. Migrations**
- Track schema changes over time
- Generate migration scripts to update the database
- Roll back changes if needed

The best ORMs strike a balance—they handle common cases elegantly while still letting you drop down to raw SQL when needed.
