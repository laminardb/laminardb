# FpML 5.13 Recommendation 2 examples

The FpML Specifications of this document are subject to the FpML Public License (the "License");
you may not use the FpML Specifications except in compliance with the License. You may obtain a
copy of the License at https://www.FpML.org.

The FpML Specifications distributed under the License are distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for the specific
language governing rights and limitations under the License.

The Licensor of the FpML Specifications is the International Swaps and Derivatives Association,
Inc. All Rights Reserved.

`confirmation/` holds the official confirmation-view examples, unmodified, downloaded on
2026-10-15 from the paths linked by the release's example index. `corpus.toml` records the
SHA-256, schema verdict, and any published erratum for each; 22 indexed files were not served.
Schema validity is not business validity: the errata list fourteen schema-valid examples that
fail FpML business rules, and `eqs-ex07-long-form-with-stub.xml` is not schema-valid
(`http://http://` is not a `NonEmptyURI`).

Fixtures written for LaminarDB tests live outside this directory and are not FpML examples.
