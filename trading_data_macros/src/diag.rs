//! What a macro error carries past its headline.
//!
//! `compile_error!` is one message on one span and nothing else — no `help`, no `note`, so every
//! sentence a reader needs next has to be crammed into the headline. `proc_macro::Diagnostic` hangs
//! them where rustc hangs its own, which is what lets the headline say the one thing that is wrong
//! and the subdiagnostics say what to write instead and why.

use core::fmt::Display;

use proc_macro2::Span;
use quote::ToTokens;

pub type Result<T> = core::result::Result<T, Diag>;

enum Sub {
	Help(String),
	Note(String),
}

pub struct Diag {
	err: syn::Error,
	/// Every token the headline underlines, joined in [`Diag::emit`]. `syn::Error` surrenders only the
	/// first of them, and a dep spelled `Buffer<C, H>` marked at `Buffer` alone points past the wrapper
	/// that is the actual mistake.
	under: Vec<Span>,
	subs: Vec<Sub>,
}

impl From<syn::Error> for Diag {
	fn from(err: syn::Error) -> Self {
		let under = vec![err.span()];
		Diag { err, under, subs: Vec::new() }
	}
}

impl Diag {
	pub fn new(span: Span, msg: impl Display) -> Self {
		syn::Error::new(span, msg).into()
	}

	pub fn spanned(tokens: impl ToTokens, msg: impl Display) -> Self {
		let ts = tokens.into_token_stream();
		let under: Vec<Span> = ts.clone().into_iter().map(|t| t.span()).collect();
		let err = syn::Error::new_spanned(ts, msg);
		match under.is_empty() {
			// `new_spanned` of nothing is `call_site`, which is still a span worth pointing at.
			true => err.into(),
			false => Diag { err, under, subs: Vec::new() },
		}
	}

	/// What to write instead.
	pub fn help(mut self, s: impl Display) -> Self {
		self.subs.push(Sub::Help(s.to_string()));
		self
	}

	/// Why it is that way — the invariant behind the headline, not a restatement of it.
	pub fn note(mut self, s: impl Display) -> Self {
		self.subs.push(Sub::Note(s.to_string()));
		self
	}

	/// Reports and expands to nothing, as `compile_error!` does: the annotated item is gone either
	/// way, and the errors that follow from its absence are the same errors.
	pub fn emit(self) -> proc_macro::TokenStream {
		let Diag { err, under, subs } = self;
		let mut rest = err.into_iter();
		let head = rest.next().expect("a `syn::Error` carries at least one message");

		let mut span = under[0].unwrap();
		for s in &under[1..] {
			// Two spans of one token stream join unless an expansion put them in different contexts, and
			// then the headline belongs on the first — the range is a nicety, the report is not.
			span = span.join(s.unwrap()).unwrap_or(span);
		}

		let mut d = proc_macro::Diagnostic::spanned(span, proc_macro::Level::Error, head.to_string());
		for s in subs {
			d = match s {
				Sub::Help(t) => d.help(t),
				Sub::Note(t) => d.note(t),
			};
		}
		d.emit();

		for e in rest {
			proc_macro::Diagnostic::spanned(e.span().unwrap(), proc_macro::Level::Error, e.to_string()).emit();
		}
		proc_macro::TokenStream::new()
	}
}
