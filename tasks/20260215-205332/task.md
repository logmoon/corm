---
title: Better logging and passing errors from db backend to user
state: closed
priority: 0
created: 20260215-205332
---
# Better logging and passing errors from db backend to user
## Logging
Add more info to what's happening, but keep the messages smart, coherent and to the point
## Errors
`backend->prepare` actually already populates an error msg from the backend when an error occures, this should be the standard for the funcs that support this sort of thing.
