using Bussig;

namespace Bussig.Tests.Unit
{
    public class MessageUrnTests
    {
        [Test]
        [Arguments(typeof(TopLevelClassWithAttribute), "urn:message:custom:name")]
        [Arguments(typeof(TopLevelRecordWithAttribute), "urn:message:top:level:record")]
        [Arguments(typeof(ITopLevelInterfaceWithAttribute), "urn:message:icustom:name")]
        [Arguments(typeof(Nester.NestedClassWithAttribute), "urn:message:nested:withattribute")]
        [Arguments(
            typeof(AnotherNamespace.TopLevelClassWithAttribute),
            "urn:message:ans:custom:name"
        )]
        [Arguments(
            typeof(AnotherNamespace.TopLevelRecordWithAttribute),
            "urn:message:ans:top:level:record"
        )]
        [Arguments(
            typeof(AnotherNamespace.ITopLevelInterfaceWithAttribute),
            "urn:message:ans:custom:iname"
        )]
        [Arguments(
            typeof(AnotherNamespace.Nester.NestedClassWithAttribute),
            "urn:message:ans:nested:with:attribute"
        )]
        public async Task ForType_WithAttribute_IsCorrect(Type type, string expected)
        {
            // Arrange & Act
            var result = MessageUrn.ForType(type);

            // Assert
            await Assert.That(result).EqualTo(expected);
        }

        [Test]
        [Arguments(typeof(TopLevelClass), "urn:message:Bussig.Tests.Unit:TopLevelClass")]
        [Arguments(typeof(TopLevelRecord), "urn:message:Bussig.Tests.Unit:TopLevelRecord")]
        [Arguments(typeof(ITopLevelInterface), "urn:message:Bussig.Tests.Unit:ITopLevelInterface")]
        [Arguments(typeof(Nester.NestedClass), "urn:message:Bussig.Tests.Unit:Nester+NestedClass")]
        [Arguments(
            typeof(TopLevelClassWithArguments<string>),
            "urn:message:Bussig.Tests.Unit:TopLevelClassWithArguments[[System:String]]"
        )]
        [Arguments(
            typeof(TopLevelClassWithArguments<TopLevelClass>),
            "urn:message:Bussig.Tests.Unit:TopLevelClassWithArguments[[Bussig.Tests.Unit:TopLevelClass]]"
        )]
        [Arguments(
            typeof(TopLevelClassWithMultipleArguments<TopLevelClass, string>),
            "urn:message:Bussig.Tests.Unit:TopLevelClassWithMultipleArguments[[Bussig.Tests.Unit:TopLevelClass],[System:String]]"
        )]
        [Arguments(
            typeof(Nester.NestedClassWithGenericArguments<
                string,
                bool,
                AnotherNamespace.ITopLevelInterface
            >),
            "urn:message:Bussig.Tests.Unit:Nester+NestedClassWithGenericArguments[[System:String],[System:Boolean],[AnotherNamespace:ITopLevelInterface]]"
        )]
        [Arguments(
            typeof(AnotherNamespace.TopLevelClass),
            "urn:message:AnotherNamespace:TopLevelClass"
        )]
        [Arguments(
            typeof(AnotherNamespace.TopLevelRecord),
            "urn:message:AnotherNamespace:TopLevelRecord"
        )]
        [Arguments(
            typeof(AnotherNamespace.ITopLevelInterface),
            "urn:message:AnotherNamespace:ITopLevelInterface"
        )]
        [Arguments(
            typeof(AnotherNamespace.Nester.NestedClass),
            "urn:message:AnotherNamespace:Nester+NestedClass"
        )]
        [Arguments(
            typeof(AnotherNamespace.TopLevelClassWithArguments<TopLevelClass>),
            "urn:message:AnotherNamespace:TopLevelClassWithArguments[[Bussig.Tests.Unit:TopLevelClass]]"
        )]
        [Arguments(
            typeof(AnotherNamespace.TopLevelClassWithMultipleArguments<
                TopLevelClass,
                AnotherNamespace.TopLevelClass
            >),
            "urn:message:AnotherNamespace:TopLevelClassWithMultipleArguments[[Bussig.Tests.Unit:TopLevelClass],[AnotherNamespace:TopLevelClass]]"
        )]
        public async Task ForType_WithoutAttribute_IsCorrect(Type type, string expected)
        {
            // Arrange & Act
            var result = MessageUrn.ForType(type);

            // Assert
            await Assert.That(result).EqualTo(expected);
        }

        [Test]
        public async Task ForType_WithGenericTypeArguments_Throws()
        {
            // Arrange & Act
            var action = () => Task.FromResult(MessageUrn.ForType(typeof(Dictionary<,>)));

            // Assert
            await Assert.ThrowsAsync<ArgumentException>(action);
        }

        [Test]
        public async Task ForType_WithAttributeWithPrefix_ReplacesPrefix()
        {
            // Arrange & Act
            var result = MessageUrn.ForType<ClassWithAttributeWithPrefix>();

            // Assert
            await Assert.That(result).EqualTo("urn:message:someurn");
        }
    }

    public class TopLevelClass;

    public record TopLevelRecord;

    public interface ITopLevelInterface;

    [MessageMapping("custom:name")]
    public class TopLevelClassWithAttribute;

    [MessageMapping("top:level:record")]
    public record TopLevelRecordWithAttribute;

    [MessageMapping("icustom:name")]
    public interface ITopLevelInterfaceWithAttribute;

    public class TopLevelClassWithArguments<TValue>
        where TValue : class;

    public class TopLevelClassWithMultipleArguments<T1, T2>
        where T1 : class
        where T2 : class;

    [MessageMapping("urn:message:someurn")]
    public class ClassWithAttributeWithPrefix;

    public class Nester
    {
        public class NestedClass;

        [MessageMapping("nested:withattribute")]
        public class NestedClassWithAttribute;

        public class NestedClassWithGenericArguments<T1, T2, T3>;
    }
}

namespace AnotherNamespace
{
    public class TopLevelClass;

    public record TopLevelRecord;

    public interface ITopLevelInterface;

    [MessageMapping("ans:custom:name")]
    public class TopLevelClassWithAttribute;

    [MessageMapping("ans:top:level:record")]
    public record TopLevelRecordWithAttribute;

    [MessageMapping("ans:custom:iname")]
    public interface ITopLevelInterfaceWithAttribute;

    public class TopLevelClassWithArguments<TValue>
        where TValue : class;

    public class TopLevelClassWithMultipleArguments<T1, T2>
        where T1 : class
        where T2 : class;

    public class Nester
    {
        public class NestedClass;

        [MessageMapping("ans:nested:with:attribute")]
        public class NestedClassWithAttribute;
    }
}
