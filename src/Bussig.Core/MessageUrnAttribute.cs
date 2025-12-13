namespace Bussig.Core;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface)]
public class MessageUrnAttribute : Attribute
{
    public MessageUrn Urn { get; private set; }

    public MessageUrnAttribute(string urn)
    {
        Urn = new MessageUrn(urn);
    }
}
