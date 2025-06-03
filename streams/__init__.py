from tap_sample.streams.projects import Projects
from tap_sample.streams.branches import Branches
from tap_sample.streams.commits import Commits
from tap_sample.streams.issues import Issues
from tap_sample.streams.project_milestones import ProjectMilestones
from tap_sample.streams.group_milestones import GroupMilestones
from tap_sample.streams.users import Users
from tap_sample.streams.groups import Groups

STREAMS = {
    "projects": Projects,
    "branches": Branches,
    "commits": Commits,
    "issues": Issues,
    "project_milestones": ProjectMilestones,
    "group_milestones": GroupMilestones,
    "users": Users,
    "groups": Groups,
}
