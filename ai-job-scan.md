# AI Job Search Evaluation Prompt

## Configuration (Update These Easily)

Target Salary Range: **$300k–$400k minimum total compensation**

Work Location Preference:  
- **Remote preferred**
- Hybrid acceptable if located in **DFW (Dallas–Fort Worth area)**

Travel Requirement:  
- **No travel preferred**
- Limited travel acceptable (<10%)

Geographic Constraint:
- Remote roles preferred
- Otherwise must be **Dallas–Fort Worth metro area**

---

# Objective

You are assisting with a **strategic job search scan**.  
Your goal is to review job listings and identify **high-value opportunities worth investigating further**.

Focus on **senior-level technical, analytics, and systems roles** where the candidate’s experience would provide strong leverage.

Avoid junior roles or roles that are clearly misaligned.

---

# Candidate Profile

Candidate background includes:

- **Network Engineering SME**
- Telecom infrastructure operations
- Vendor defect management and troubleshooting
- Incident and outage analysis
- Data analytics within network operations
- Automation and scripting
- Dashboard and operational intelligence tooling
- Cross-functional technical leadership

Key strengths:

- Combining **deep infrastructure knowledge with data analytics**
- Translating operational data into **decision-making insights**
- Diagnosing complex production issues
- Communicating with vendors and internal leadership
- Building tooling that improves operational visibility

Candidate is currently positioned between:

- **Senior Engineer**
- **Principal Engineer**
- **Operational Intelligence / Reliability Engineer**
- **Technical Architect (operations analytics)**

---

# Target Roles

Prioritize roles similar to:

- Principal Engineer
- Staff Engineer
- Distinguished Engineer
- Reliability Engineering (SRE leadership)
- Infrastructure Analytics Engineer
- Network Systems Architect
- Operational Intelligence Engineer
- AI-assisted infrastructure analytics roles
- Data-driven infrastructure operations roles

Industries of interest:

- Telecom
- Cloud infrastructure
- Data infrastructure
- Internet platforms
- Infrastructure SaaS
- Large-scale distributed systems

---

# Compensation Rules

Roles must realistically support **$300k–$400k total compensation**.

Include roles where:

- Base salary + bonus + equity likely reaches this range
- Senior technical leadership levels
- Staff / Principal levels at large tech companies

Exclude roles where:

- Total comp clearly below $250k
- Mid-level engineering positions
- Contract work
- Temporary roles

---

# Location Rules

Prioritize:

1. **Fully remote roles**
2. Roles based in **Dallas–Fort Worth**
3. Remote-first companies

Avoid roles requiring:

- Relocation outside DFW
- Heavy travel
- Frequent onsite presence

---

# Filtering Logic

Reject jobs that include:

- Mandatory travel >20%
- Sales engineering roles
- Pure management roles with no technical depth
- Entry-level or junior roles
- Contract / consulting roles

Flag jobs that appear promising but uncertain.

---

# Evaluation Criteria

Score each job from **1–10** based on the following:

### Compensation Potential
Likelihood of reaching the target salary band.

### Role Leverage
How much the role benefits from a combination of:

- infrastructure expertise
- analytics
- operational systems thinking

### Autonomy
Roles with strong technical ownership score higher.

### Remote Compatibility
Remote-first companies score higher.

### Career Trajectory
Roles that advance toward **principal architect or technical strategy** positions score higher.

---

# Output Format

Return results in the following format:

## Top Matches

### Job Title
Company  
Location  

Estimated Total Compensation Range  

Why this role fits  

Score: X/10

---

## Potential Matches (Needs Review)

### Job Title
Company  

Reason it might fit  
Potential concerns  

Score: X/10

---

## Rejected Roles

List briefly with reason:

- compensation too low
- location mismatch
- role type mismatch

---

# Additional Guidance

If you detect **patterns in the job market** (for example, increasing demand for certain skill sets), summarize those insights.

Examples:

- AI + infrastructure roles increasing
- Data-driven reliability engineering demand
- Automation-heavy network operations roles

Provide a short **market insight section** if relevant.

---

# Final Deliverable

Provide:

1. Top 5–10 strong opportunities
2. 5–10 possible opportunities
3. Market insight summary
