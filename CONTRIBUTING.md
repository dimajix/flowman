# Contributing to Flowman

You want to contribute to Flowman? Welcome! Please read this document to understand what you can do:
 * [Report an Issue](#report-an-issue)
 * [Contribute Documentation](#contribute-documentation)
 * [Contribute Code](#contribute-code)


## Report an Issue

If you find a bug - behavior of Flowman code contradicting your expectation - you are welcome to report it.
We can only handle well-reported, actual bugs, so please follow the guidelines below.

Once you have familiarized with the guidelines, you can go to the [GitHub issue tracker for Flowman](https://github.com/dimajix/flowman/issues/new) to report the issue.

### Quick Checklist for Bug Reports

Issue report checklist:
 * Real, current bug
 * No duplicate
 * Reproducible
 * Good summary
 * Well-documented
 * Minimal example

### Issue handling process

When an issue is reported, a committer will look at it and either confirm it as a real issue, close it if it is not an issue, or ask for more details.

An issue that is about a real bug is closed as soon as the fix is committed.

### Usage of Labels

GitHub offers labels to categorize issues. We suggest the following labels:

Labels for issue categories:
 * bug: this issue is a bug in the code
 * feature: this issue is a request for a new functionality or an enhancement request
 * environment: this issue relates to supporting a specific runtime environment (Cloudera, specific Spark/Hadoop version, etc)

Status of open issues:
 * help wanted: the feature request is approved and you are invited to contribute

Status/resolution of closed issues:
 * wontfix: while acknowledged to be an issue, a fix cannot or will not be provided

### Issue Reporting Disclaimer

We want to improve the quality of Flowman and good bug reports are welcome! But our capacity is limited, thus we reserve the right to close or to not process insufficient bug reports in favor of those which are very cleanly documented and easy to reproduce. Even though we would like to solve each well-documented issue, there is always the chance that it will not happen - remember: Flowman is Open Source and comes without warranty.

Bug report analysis support is very welcome! (e.g. pre-analysis or proposing solutions)



## Contribute Documentation

Flowman has many features implemented, unfortunately not all of them are well documented. So this is an area where we highly welcome contributions from users in order to improve the documentation. The documentation is contained in the "doc" subdirectory within the source code repository. This implies that when you want to contribute documentation, you have to follow the same procedure as for contributing code.



## Contribute Code

You are welcome to contribute code to Flowman in order to fix bugs or to implement new features.

There are three important things to know:

1.  You must be aware of the Apache License (which describes contributions) and **agree to the Contributors License Agreement**. This is common practice in all major Open Source projects.
 For company contributors special rules apply. See the respective section below for details.
2.  Please ensure your contribution adopts Flowmans **code style, quality, and product standards**. The respective section below gives more details on the coding guidelines.
3.  **Not all proposed contributions can be accepted**. Some features may e.g. just fit a third-party plugin better. The code must fit the overall direction of Flowman and really improve it. The more effort you invest, the better you should clarify in advance whether the contribution fits: the best way would be to just open an issue to discuss the feature you plan to implement (make it clear you intend to contribute).

### Contributor License Agreement

When you contribute (code, documentation, or anything else), you have to be aware that your contribution is covered by the same [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0) that is applied to Flowman itself.

In particular, you need to agree to the [Flowman Contributors License Agreement](https://cla-assistant.io/dimajix/flowman), stating that you have the right and are okay to put your contribution under the license of this project.
CLA assistant will ask you to confirm that.

This applies to all contributors, including those contributing on behalf of a company.
If you agree to its content, you simply have to click on the link posted by the CLA assistant as a comment to the pull request. Click it to check the CLA, then accept it on the following screen if you agree to it.
CLA assistant will save this decision for upcoming contributions and will notify you if there is any change to the CLA in the meantime.

### Contribution Content Guidelines

These are some rules we try to follow:

-   Apply a clean coding style adapted to the surrounding code, even though we are aware the existing code is not fully clean
-   Use (4)spaces for indentation
-   Use variable naming conventions like in the other files you are seeing (camelcase)
-   No println - use SLF4J logging instead
-   Comment your code where it gets non-trivial
-   Write a unit test
-   Do not do any incompatible changes, especially do not change or remove existing properties from YAML specs

### How to contribute - the Process

1.  Make sure the change would be welcome (e.g. a bugfix or a useful feature); best do so by proposing it in a GitHub issue
2.  Create a branch forking the flowman repository and do your change
3.  Commit and push your changes on that branch
4.  If your change fixes an issue reported at GitHub, add the following line to the commit message:
    - ```Fixes #(issueNumber)```
5.  Create a Pull Request with the following information
    - Describe the problem you fix with this change.
    - Describe the effect that this change has from a user's point of view. App crashes and lockups are pretty convincing for example, but not all bugs are that obvious and should be mentioned in the text.
    - Describe the technical details of what you changed. It is important to describe the change in a most understandable way so the reviewer is able to verify that the code is behaving as you intend it to.
6.  Follow the link posted by the CLA assistant to your pull request and accept it, as described in detail above.
7.  Wait for our code review and approval, possibly enhancing your change on request
    -   Note that the Flowman developers also have their regular duties, so depending on the required effort for reviewing, testing and clarification this may take a while
8.  Once the change has been approved we will inform you in a comment
9.  We will close the pull request, feel free to delete the now obsolete branch
