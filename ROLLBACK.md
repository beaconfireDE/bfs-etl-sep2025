# Rollback Documentation

## Overview
This document explains the rollback of PR #16 that was accidentally merged to the `main` branch.

## What Happened
- **Date**: October 30, 2025
- **PR**: #16 "drafting the dag for group 3"  
- **Merge Commit**: `1bab0cedf86cbeb80bab72efd7d815f3fa41bb6f`
- **Issue**: This PR was merged to `main` but should have been kept on the `team3` branch

## Files Affected
The following files were added by the merge and have been removed:
- `dags/complex_example.py`
- `dags/dev_db_test.py`
- `dags/dev_db_test.sql`
- `dags/empty_workflow_example.py`
- `dags/s3_data_copy_test.py`
- `project_2_dag.py`

## Rollback Process
1. Identified the merge commit: `1bab0cedf86cbeb80bab72efd7d815f3fa41bb6f`
2. Identified the pre-merge state: `dc4d6d4fee4832c215f21be9abc382b9ea3768ed`
3. Removed all files added by the merge
4. Created this documentation

## How to Prevent This in the Future
1. **Branch Protection**: Enable branch protection rules on `main` to require pull request reviews
2. **Review Process**: Ensure at least one reviewer approves changes before merging
3. **Branch Naming**: Use clear branch naming conventions (e.g., `feature/`, `team/`)
4. **Documentation**: Clearly document which branches should merge to `main`

## Recovery
The work from PR #16 still exists on the `team3` branch (`ce8e0721e43fca96c52c5c7103ae277636d32a2f`) and can be merged to the appropriate target branch when ready.

## Related Branches
- `main`: Rolled back to pre-merge state
- `team3`: Contains the work that was prematurely merged
- `team1`, `team2`: Other team branches (unaffected)
