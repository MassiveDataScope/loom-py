from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, get_args, get_origin

from typing_extensions import TypeVar as TypeVarExt

from loom.core.model import LoomStruct
from loom.core.repository.abc import RepoFor
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.engine.plan import ExecutionPlan

ModelT = TypeVar("ModelT")
ResultT = TypeVar("ResultT")
RepoT = TypeVarExt("RepoT", default=RepoFor[Any])


class UseCase(ABC, Generic[ModelT, ResultT, RepoT]):
    """Base class for all use cases.

    Subclass and implement ``execute`` with typed parameters. Parameter
    defaults declare the execution contract:

    - ``Input()``  — command payload, built from the raw request.
    - ``LoadById(EntityType, by="param")`` — entity prefetched by id.
    - ``Load(EntityType, ...)`` — entity prefetched by arbitrary field.
    - ``Exists(EntityType, ...)`` — boolean existence check by field.
    - No default — primitive param bound directly from the caller.

    Class attributes ``computes``, ``rules``, and ``read_only`` declare the
    pre-execution pipeline and execution policy.  They are inspected once at
    startup by ``UseCaseCompiler`` and embedded in the immutable
    ``ExecutionPlan``.

    Attributes:
        computes: Compute transformations applied in order before rule checks.
        rules: Rule validations applied in order after computes.
        read_only: When ``True``, the executor skips opening a
            ``UnitOfWork`` transaction.  Set this on query-only use cases
            that never mutate state.  GET routes in :class:`RestInterface`
            always bypass the UoW regardless of this flag.

    Example::

        class UpdateUserUseCase(UseCase[User, UserResponse]):
            computes = [set_updated_at]
            rules = [email_must_be_valid]

            def __init__(self, user_repo: UserRepository) -> None:
                self._user_repo = user_repo

            async def execute(
                self,
                user_id: int,
                cmd: UpdateUserCommand = Input(),
                user: User = LoadById(User, by="user_id"),
            ) -> UserResponse:
                ...
    """

    __execution_plan__: ClassVar[ExecutionPlan | None] = None
    __loom_main_model__: ClassVar[type[LoomStruct] | None] = None
    __loom_main_repo_contract__: ClassVar[object] = RepoFor[Any]
    computes: ClassVar[Sequence[ComputeFn[Any]]] = ()
    rules: ClassVar[Sequence[RuleFn]] = ()
    read_only: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Normalise the declared main-repository contract once per subclass."""
        super().__init_subclass__(**kwargs)
        model, repo_contract = cls._resolve_main_repo_contract()
        cls.__loom_main_model__ = model
        cls.__loom_main_repo_contract__ = repo_contract

    def __init__(self, main_repo: RepoT | None = None) -> None:
        """Initialise the use case base dependencies.

        Args:
            main_repo: Optional main repository dependency. When provided, the
                factory may inject it from ``RepoFor[Model]`` constructor
                annotations.
        """
        self._main_repo = main_repo

    @property
    def main_repo(self) -> RepoT:
        """Main repository injected by the factory."""
        if self._main_repo is None:
            raise RuntimeError(
                f"{type(self).__qualname__} requires a main repository but it was not injected."
            )
        return self._main_repo

    @main_repo.setter
    def main_repo(self, value: RepoT) -> None:
        self._main_repo = value

    @classmethod
    def describe_main_repo(cls) -> tuple[type[LoomStruct] | None, object]:
        """Return the normalized main-repository contract for this use case."""
        return cls.__loom_main_model__, cls.__loom_main_repo_contract__

    @classmethod
    def _resolve_main_repo_contract(cls) -> tuple[type[LoomStruct] | None, object]:
        """Resolve main model and repository contract from declared generics."""
        for base in getattr(cls, "__orig_bases__", ()):
            if get_origin(base) is not UseCase:
                continue
            args = get_args(base)
            if len(args) not in {2, 3}:
                raise TypeError(
                    f"{cls.__qualname__} must declare UseCase[TModel, TResult] or "
                    f"UseCase[TModel, TResult, TRepo]. Got {len(args)} generic parameter(s)."
                )

            model = args[0]
            if model in {Any, object}:
                return None, RepoFor[Any]
            if not isinstance(model, type) or not issubclass(model, LoomStruct):
                raise TypeError(f"{cls.__qualname__}: TModel must be a LoomStruct subclass.")
            if len(args) == 3:
                return model, args[2]
            return model, RepoFor[Any]
        return None, RepoFor[Any]

    @abstractmethod
    async def execute(self, *args: Any, **kwargs: Any) -> ResultT:
        """Execute core business logic.

        Override with an explicit typed signature. The compiler inspects
        this method once at startup to build the ``ExecutionPlan``.

        Returns:
            The result of the use case operation.
        """
        ...
