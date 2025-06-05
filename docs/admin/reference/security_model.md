# DiracX Token Security Model

Version: v0.9.0

## Table of Contents

1. [Introduction](#introduction)

    - [Terms and Definitions](#terms-and-definitions)

2. [DiracX Authorisation and Authentication](#diracx-authorisation-and-authentication)

    - [User and Group Management](#user-and-group-management)
    - [Lifetime](#lifetime)
    - [Token Profile](#token-profile)
    - [Signature Verification](#signature-verification)
    - [Issuance](#issuance)
        - [Supported Authorisation Flows](#supported-authorisation-flows)
    - [Pilot Jobs](#pilot-jobs)
    - [Installation Administrators](#installation-administrators)

3. [External Authorization and Authentication](#external-authorization-and-authentication)

    - [Storage Access](#storage-access)
    - [Computing Resources](#computing-resources)

4. [Network Communication](#network-communication)

    - [Certificate Signing](#certificate-signing)

5. [Threat Analysis](#threat-analysis)

    - [Compromised External IdP](#compromised-external-idp)
    - [Compromised Batch Submission System](#compromised-batch-submission-system)
    - [Compromised Worker Node](#compromised-worker-node)
    - [Compromised Refresh Token](#compromised-refresh-token)
    - [Compromised User Identity](#compromised-user-identity)
    - [Compromised JWK](#compromised-jwk)
    - [Compromised DB](#compromised-db)
    - [Compromised Hosts](#compromised-hosts)
    - [Malicious Legitimate User](#malicious-legitimate-user)

6. [Legacy Compatibility and Migration](#legacy-compatibility-and-migration)

7. [Changelog](#changelog)

## Introduction

DIRAC provides the tools to provide a uniform interface to access heterogeneous storage and compute to one or more "Virtual Organisations". DiracX is the next evolution of DIRAC and is built around token-based authentication.

This document describes how DiracX enables installations to be secure and robust against a wide variety of threats. Except for the legacy compatibility section the document is written in the present tense however, it does not yet reflect the current status of DiracX.

### Terms and Definitions

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

Commonly used terms in this document are described below.

- **External IdP:** The Identity Provider that is responsible for managing a Virtual Organisation's users.
- **Group:** A DiracX-specific extension to a user's identity that enables additional access control features. A user may be a member of many groups but only ever has one associated with their current identity. All the members of a group belong to the same Virtual Organisation.
- **Identity:** The subject and their currently chosen group.
- **Installation administrator:** A person responsible for managing the DiracX installation itself.
- **Installation:** A single deployment of DiracX services.
- **Pilot:** A job submitted to a computing resource's batch system. DiracX then uses this to run any number of DiracX jobs on behalf of users in a Virtual Organisation.
- **Capability:** A string which represents a specific authorisation within DiracX.
- **User:** An identity which is registered within a Virtual Organisation that is part of the DiracX installation.
- **Virtual Organisation:** A set of users that is defined around a common set of resource-sharing rules and conditions.

## DiracX authorisation and authentication

DiracX is built around the OAuth 2.0 authorisation flow, with the exception that an identity MUST always be present alongside authorisation to enable user-specific behaviours such as:

- Limiting file access to a user's/group's files (POSIX style)
- Enforcing user/group-specific quotas on compute and storage resources
- Allowing for easier traceability

For communication between users and DiracX services JSON Web Tokens (JWTs) are used as OAuth2 bearer tokens. The same mechanism is used whenever impersonation is required, such as running a user-provided payload in a job. The need to use encrypted JWT is not foreseen.

### User and group management

Members of a given Virtual Organisation are defined in the external IdP for the given organisation. When a user first interacts with DiracX they are automatically registered in DiracX.

Group membership can be handled in one of two ways.
By default group membership is handled inside of DiracX independently of the external IdP. In this situation, each time a new user is encountered they are automatically registered in zero or more groups. This behaviour is configured by the installation administrator.

Alternatively, group membership can be fully delegated to the external IdP. This is most useful for VOs which have an existing user and group management system, such as VOs associated with CERN. There is no mechanism for partially offloading group membership within a single VO.

### Lifetime

DiracX uses access tokens to carry a user's identity and a limited set of permissions. The token can be verified without a central service. These tokens cannot be revoked and are only issued with a short lifetime (installation dependent, SHOULD be order minutes).

Refresh tokens have a longer lifetime and are verified by a central service. Refresh token rotation is used to further improve security. These tokens can be revoked by users, installation admins and automated processes.

### Token profile

The access token profile contains the standardised fields from RFC-7519 (`sub`, `aud`, `iss`, `jti`) and OpenID Connect Core 1.0 (`preferred_username`). The `sub` field is unique per person per VO and there is a single issuer per installation. In addition, the token contains DiracX-specific fields:

- `vo`: String containing the name of the virtual Organisation to which the user belongs. This field may change if it becomes a registered field following the merge of WLCG, EGI and SciTokens profiles, in which case we would adapt.
- `dirac_group`: String containing the name of the current group that the user is acting as a member of. This is used only for identity purposes.
- `dirac_capabilities`: A list of strings representing the DiracX-specific permissions for authentication.

These fields are requested using the `scope` parameter when initiating the OAuth2 flow.

### Signature verification

DiracX exclusively uses asymmetric cryptography for signing tokens. It follows the current best practices for algorithms and key lengths and supports multiple active keys at the same time to facilitate key rotation. The time required before the key can be invalidated without disruption is related to the installation configured access token duration.

The JSON Web Keys required for verifying tokens are publicly exposed according to OIDC standards.

### Issuance

Users need to be issued tokens in two main contexts:

- within a web browser for the DIRAC web portal
- within an interactive terminal session

There MAY also be other contexts in which can be issued tokens on a per-installation basis. This mechanism is very generic, and only relies on being able to receive a proof of identity from a trusted source. Some examples include:

- Exchange an ID token generated by a continuous integration provider (e.g. GitLab, GitHub). These providers might be limited to read-only data access and the DiracX tokens lifetime SHOULD NOT exceed the lifetime of the CI job.
- Exchange an ID token which is provided automatically to a Jupyter instance such as is already used by several analysis facility prototypes.
- Using alternative mechanisms such as Kerberos to verify a user's identity. This case might be configured to only support issuing tokens with commonly required user capabilities and be prevented from accessing more powerful capabilities.

In these cases, DiracX provides options to limit the contents of tokens which are issued, such as the group, capabilities and lifetime. Installation admins MUST take care to ensure to carefully consider which additional issuance mechanisms to support.

Each VO in DiracX is associated with an external identity provider which MUST:

- Support the authorization code flow with Proof Key of Code Exchange
- Only issue identity tokens for users which are intended to have access to the VO's resources

#### Supported authorisation flows

DiracX issues access and refresh tokens via three OAuth2 authorisation flows:

- **Authorization Code with Proof Key of Code Exchange:** This is primarily used for the DiracX web portal.
- **Device Authorization Flow with Proof Key of Code Exchange:** This is primarily used for terminal access to DiracX services.
- **Refresh Token Grant:** This is used with the refresh tokens issued with the other flows.
- **Token Exchange Grant:** Enables trusted externally issued tokens to be exchanged for DiracX credentials.

There is currently no need foreseen for DiracX to issue `id_tokens` as all the information is already contained in the access token.

All flows involving the external IdP follow the same sequence:

![](https://user-images.githubusercontent.com/3728211/274624748-aaa0c6c1-9bcf-4c89-8344-4597b3e15cfe.png)

### Pilot jobs

Pilots are submitted with a unique secret which can be used to start the pilot. This secret is hashed and registered in a database and can only be used to start a pilot only once. In special cases (such as Vacuum) a secret can be registered in the database manually by an installation admin for use with multiple pilots.

This initial secret is exchanged for a credential that can be used for pilot-specific activities, such as matching jobs or uploading pilot logging information, henceforth referred to as the pilot-matching secret.

When matching jobs, the pilot will be provided with the limited credential required to run the job payload on behalf of the user. This credential will be provided with an installation-defined set of minimal capabilities and an appropriate lifetime to finish the job. Upon job completion, the refresh token will be invalidated.

### Installation administrators

Normal users cannot observe activity outside of their single VO however for installation administrators it is necessary to have an overview of the entire system. To facilitate this DiracX always has an internal VO for administration. This VO is configured like a standard VO with an external IdP however it is entirely internal to DiracX and cannot be associated with resources. It only has access to endpoints which are necessary to support individual VOs and configure DiracX itself. For example, admins can kill any job within the system but cannot submit new jobs.

## External authorization and authentication

DiracX primarily gives access to two types of resources: storage and compute.

### Storage Access

The new model for accessing storage resources has not yet reached consensus in the wider community therefore this section is deferred to a later point in time. For now, data access will be handled with the X509-based methods described in the legacy compatibility section.

### Computing resources

Credentials to access external computing resources are only used for pilot submission and are signed with different keys to those used by DiracX's own internal tokens. Installations SHOULD NOT allow users to submit jobs directly to external computing resources.

## Network communication

Communication between components is performed using HTTP and DiracX does not directly provide any mechanisms to encrypt or otherwise protect data sent over the network. Installations MUST be configured to use only secure variants of HTTP (such as using a load balancer and edge termination or directly configuring SSL in their Asynchronous Server Gateway Interface).

### Certificate signing

Installations SHOULD use certificates signed by a widely trusted root certificate authority to:

- Ensure clients can be securely bootstrapped
- Prevent users from being encouraged to bypass certificate warnings when interacting with DiracX

These certificates SHOULD be generated in accordance with best practices at the time.

Custom Certificate Authorities (CAs) may be used for communicating with external resources. DiracX provides mechanisms for securely distributing the necessary information such that clients can securely access resources which are authenticated with non-standard CAs.

## Threat Analysis

Here we consider a variety of scenarios where malicious parties attempt to access DiracX. Assuming an attacker has already compromised the system, DiracX is designed to avoid privilege escalation opportunities and provide installation admins with the means of responding. In all cases, installation admins should reach out to any local security teams (following e.g. Sirtfi, Snctfi, ...) they have available to refer them to this document and seek further advice. The administrators of the concerned VO should also be contacted, in concertation with the security teams.

### Compromised external IdP

If an IdP has been compromised then DiracX no longer has a means of verifying user identities. In this situation:

- The VO is banned within DiracX and can no longer be used
- All refresh tokens for the VO are revoked
- Review all activity from the VO

### Compromised batch submission system

If an attacker gains access to the information held within a site's batch submission system the only secret information is the per-pilot secrets. Each secret has a limited lifetime and can only be used once, therefore only secrets for waiting pilots have any value.

Depending on the nature of the compromise, installation admins SHOULD:

- Stop submission of new pilots to the resource
- Invalidate potentially affected pilot secrets
- Invalidate any refresh tokens which originated from a leaked pilot secret
- Invalidate any pilot-matching secrets which originated from a leaked pilot secret
- Review any activity that was done with potentially compromised access tokens

### Compromised worker node

If one or more worker nodes are compromised the refresh tokens granted to them are no longer be trusted. In this situation installation admins MUST:

- Invalidate any refresh tokens which were sent to the given worker node
- Invalidate the pilot matching secret
- Review any activity that was done with potentially compromised access tokens

### Compromised refresh token

If a user's refresh token is exposed they MUST be able to revoke it independently. During the revocation, DiracX will encourage them to inform the installation administrator with relevant information if the nature of the compromise is notable.

### Compromised user identity

If a user's identity is compromised the installation admin MUST:

- Block the user within DiracX
- Ask the IdP to block the user and reestablish their identity
- Revoke all refresh tokens granted to that user
- Kill any waiting jobs within the system
- Review all user activity

### Compromised JWK

In the event that the JWK used for signing DIRAC tokens is compromised installation admins MUST generate new keys immediately and cease to use the previous public keys. In addition, they SHOULD:

- Consider shutting down the instance entirely
- Review the DiracX configuration service for malicious changes
- Review all user activity

### Compromised DB

A compromised DB depends on the nature of the compromise (read-only or read-write) and the exact DBs involved. Ready-only access to a database doesn't present a security concern, only a privacy issue.

For databases containing secrets (e.g. pilot secrets), the secrets are hashed such that even an attacker with read access to the DB cannot use the tokens to access DiracX services. Additionally, a per installation secret salt ("pepper") is used so even an attacker with means to modify the DB contents cannot insert new valid values.

If the attacker has the means to modify the DB contents and installation admins SHOULD:

- Change DB credentials
- Review what activity might be possible as the result of access to this DB. For example, modifying the job database would enable arbitrary payload execution and the exfiltration of limited-scope user refresh tokens.

### Compromised hosts

In the event of a host being compromised it MUST be treated as equivalent to the sections previously described:

- Compromised all JWKs
- Compromised all DBs

Additionally:

- Any secrets DiracX services have access to (e.g. salts for hashing, passwords) MAY increase the severity of the compromise.
- At a minimum the host SHOULD be reinstalled, though replacing the hardware might be required depending on the nature of the attacker.

### Malicious legitimate user

DiracX strives to adhere to the principle of least privilege therefore these models also apply with the view to limiting the impact of a legitimate party acting maliciously. A malicious legitimate user is therefore equivalent to a compromised user from a technical perspective. The logging and traceability provided by DiracX should help installation admins disentangle the two situations.

## Legacy compatibility and migration

DiracX is intended to provide a seamless migration for existing DIRAC installations. To facilitate this whenever a refresh token is issued a proxy will also be issued with a matching identity and lifetime. Running in this configuration means the user MUST have previously delegated a long-lived proxy to the installation.

Likewise, whenever a proxy is generated a corresponding refresh token will also be issued.

In the event of malicious activity, refer to existing procedures in addition to what was previously described.

## Changelog

- v0.1.0 (2023-06-07): Initial draft for internal review.
- v0.2.0 (2023-06-12): Add glossary of common terms.
- v0.3.0 (2023-06-27):
    - Define Identity
    - Specify token issuer
    - Change property to capability
    - Add diagram of the role of the external IdP, DiracX and the user
- v0.9.0 (2023-10-11):
    - Specify traceability consideration
    - Specify reference of the registered claim
    - `vo` field in the token may change if it becomes a standard
    - Better description of alternative mechanism to issue a token
    - Explicit the reason for not needing to issue `id_token`
    - Recommend to contact VO admins in case of compromission
