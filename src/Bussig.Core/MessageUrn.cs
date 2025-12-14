using System.Reflection;
using System.Text;

namespace Bussig.Core;

public readonly record struct MessageUrn
{
    private const string Prefix = "urn:message:";
    private readonly string _valueWithoutPrefix;

    public MessageUrn(string value)
    {
        if (value.StartsWith(Prefix))
        {
            _valueWithoutPrefix = value.Replace(Prefix, string.Empty);
            return;
        }

        _valueWithoutPrefix = value;
    }

    public static MessageUrn ForType<T>() => ForType(typeof(T));

    public static MessageUrn ForType(Type type)
    {
        if (type.ContainsGenericParameters)
        {
            throw new ArgumentException(
                "A message type cannot contain generic parameters",
                nameof(type)
            );
        }

        var messageUrn = GetMessageUrnFromAttribute(type);

        return messageUrn
            ?? new MessageUrn(GetMessageNameFromType(new StringBuilder(), type, true));
    }

    private static MessageUrn? GetMessageUrnFromAttribute(Type type)
    {
        var attribute = type.GetCustomAttribute<MessageMappingAttribute>();
        return attribute?.Urn;
    }

    private static string GetMessageNameFromType(StringBuilder sb, Type type, bool includeScope)
    {
        if (type.IsGenericParameter)
            return string.Empty;

        var ns = type.Namespace;
        if (includeScope && ns is not null)
        {
            sb.Append(ns).Append(':');
        }

        if (type is { IsNested: true, DeclaringType: not null })
        {
            GetMessageNameFromType(sb, type.DeclaringType, false);
            sb.Append('+');
        }

        if (type.IsGenericType)
        {
            var name = type.GetGenericTypeDefinition().Name;

            var index = name.IndexOf('`');
            if (index > 0)
            {
                name = name.Remove(index);
            }

            sb.Append(name);
            sb.Append('[');

            var arguments = type.GetGenericArguments();
            foreach (var (i, argument) in arguments.Index())
            {
                if (i > 0)
                    sb.Append(',');

                sb.Append('[');
                GetMessageNameFromType(sb, argument, true);
                sb.Append(']');
            }

            sb.Append(']');
        }
        else
        {
            sb.Append(type.Name);
        }

        return sb.ToString();
    }

    public override string ToString() => $"{Prefix}{_valueWithoutPrefix}";

    public static implicit operator string(MessageUrn messageUrn) => messageUrn.ToString();
}
