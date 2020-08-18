function removeEscapeCharactersForValue(value) {
    const regExp = new RegExp('"', "g");
    const regExpBackSlash = new RegExp(/[\\]|[,]/, "g");
    return "\"" + value.replace(regExp, '""').replace(regExpBackSlash, "") + "\"";
}

export {removeEscapeCharactersForValue};